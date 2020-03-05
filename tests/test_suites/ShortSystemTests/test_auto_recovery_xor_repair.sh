timeout_set 5 minutes
CHUNKSERVERS=4 \
	USE_RAMDISK=YES \
	CHUNKSERVER_LABELS="0,1,2:X|3:B" \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota" \
	MASTER_EXTRA_CONFIG="MAGIC_DISABLE_METADATA_DUMPS = 1`
			`|AUTO_RECOVERY = 1`
			`|CHUNKS_LOOP_MIN_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|CHUNKS_SOFT_DEL_LIMIT = 10`
			`|CHUNKS_WRITE_REP_LIMIT = 10`
			`|OPERATIONS_DELAY_INIT = 0`
			`|OPERATIONS_DELAY_DISCONNECT = 0" \
	MASTER_CUSTOM_GOALS="10 xor2 : \$xor2 {X X X}`
			`|11 backup : B" \
	setup_local_empty_lizardfs info

# Remember version of the metadata file. We expect it not to change when generating data.
metadata_version=$(metadata_get_version "${info[master_data_path]}"/metadata.mfs)

cd ${info[mount0]}
# Create file and make sure it has all xor chunks on CS 0, 1 and 2.
mkdir dir
lizardfs setgoal xor2 dir
FILE_SIZE=$(( 4 * LIZARDFS_CHUNK_SIZE )) file-generate dir/file
for cs in {0..2}; do
	assert_equals 4 $(find_chunkserver_chunks $cs -name "chunk_xor*" | wc -l)
done

# Make snapshot of the file and wait until it has ordinary chunks on CS 3.
mkdir backup
lizardfs makesnapshot dir/file backup/snapshot
lizardfs setgoal backup backup/snapshot
assert_eventually_prints 4 'find_chunkserver_chunks 3 -name "chunk_0*" | wc -l'

# Make sure next chunk replications won't happen.
sed -ie 's/OPERATIONS_DELAY_INIT = 0/OPERATIONS_DELAY_INIT = 300/' "${info[master_cfg]}"
lizardfs_master_daemon reload

# Stop CS 3 (non-xor), and remove third chunk from it.
lizardfs_chunkserver_daemon 3 stop
chunk=$(find_chunkserver_chunks 3 -name "chunk_*0000000000000003_00000001.???")
assert_success rm "$chunk"

# Update first and second chunk of the file, this will change ids on CS 0, 1 and 2 because of snapshot.
dd if=/dev/zero of=dir/file conv=notrunc bs=32KiB count=$((2*1024 + 10))
# dir/file will have the following chunks:
#   +-----------------+-----------------+-----------------+-----------------+
#   | chunk 0 [id: 5] | chunk 1 [id: 6] | chunk 2 [id: 3] | chunk 3 [id: 4] |
#   +-----------------+-----------------+-----------------+-----------------+
for CS in {0..2}; do
	for chunk in 1 2; do
		# Chunk id will be incremented by number of chunks.
		((chunk = chunk + 4))
		chunk_name="chunk_xor*000000000000000${chunk}_00000001.???"
		assert_equals 1 $(find_chunkserver_chunks $CS -name "$chunk_name" | wc -l)
	done
done

# Stop CS 0 and remove fourth chunk from it.
lizardfs_chunkserver_daemon 0 stop
chunk=$(find_chunkserver_chunks 0 -name "chunk_xor*0000000000000004_00000001.???")
assert_success rm $chunk

# Situation with readable old version of xor chunks is simulated by keeping old version of xor chunk.
# Copy fifth chunk from CS 1.
saved_chunk=$(find_chunkserver_chunks 1 -name "chunk_xor*0000000000000005_00000001.???")
cp "$saved_chunk" "$TEMP_DIR"

# Update first chunk of the file, this will change it's version to 2 on CS 1 and 2.
dd if=/dev/zero of=dir/file conv=notrunc bs=32KiB count=10
# Chunk id was incremented by number of chunks.
assert_equals 1 $(find_chunkserver_chunks 0 -name "chunk_xor*0000000000000005_00000001.???" | wc -l)
assert_equals 1 $(find_chunkserver_chunks 1 -name "chunk_xor*0000000000000005_00000002.???" | wc -l)
assert_equals 1 $(find_chunkserver_chunks 2 -name "chunk_xor*0000000000000005_00000002.???" | wc -l)

# Stop CS 1 and 2 and remove third and fourth chunk from it.
for CS in 1 2; do
	lizardfs_chunkserver_daemon $CS stop
	chunk=$(find_chunkserver_chunks $CS -name "chunk_xor*0000000000000003_00000001.???")
	assert_success rm "$chunk"
	chunk=$(find_chunkserver_chunks $CS -name "chunk_xor*0000000000000004_00000001.???")
	assert_success rm "$chunk"
done

# On CS 1 replace fifth chunk with saved version (id: 5 ver: 2) << (id: 5 ver: 1).
chunk=$(find_chunkserver_chunks 1 -name "chunk_xor*0000000000000005_00000002.???")
assert_success rm "$chunk"
mv "$TEMP_DIR/${saved_chunk##*/}" "$saved_chunk"

# Restart all chunkservers.
for CS in {0..3}; do
	lizardfs_chunkserver_daemon $CS start
done
lizardfs_wait_for_ready_chunkservers 4

# There should be:
# chunk 0 [0 copies] - one xor part (id: 5, ver: 2), two xor parts (id: 5 ver: 1)
# chunk 1 [1 copy  ] - three xor parts (id: 6, ver: 1)
# chunk 2 [0 copies] - one xor part (id: 3, ver: 1)
# chunk 3 [1 copy  ] - one std chunk with (id: 4 ver: 1) (from snaphot)
checkfile=$(lizardfs checkfile dir/file)
assert_awk_finds '/chunks with 0 copies: *2$/' "$checkfile"
assert_awk_finds '/chunks with 1 copy: *2$/' "$checkfile"

# Repair the file and remember metadata after the repair.
repairinfo=$(lizardfs filerepair dir/file)
fileinfo=$(lizardfs fileinfo dir/file)
assert_awk_finds '/chunks not changed: *2$/' "$repairinfo"
assert_awk_finds '/chunks erased: *1$/' "$repairinfo"
assert_awk_finds '/chunks repaired: *1$/' "$repairinfo"
# First chunk should have version 1 (from CS 1 & 2).
assert_awk_finds '/id:5 ver:1/' "$fileinfo"
# Second chunk should have version 1 (from CS 1 & 2 & 3).
assert_awk_finds '/id:6 ver:1/' "$fileinfo"
assert_awk_finds_no '/chunks with 0 copies/' "$(lizardfs checkfile dir/file)"
metadata=$(metadata_print)

# Simulate crash of the master
cd
lizardfs_master_daemon kill

# Make sure changes are in changelog only (ie. that metadata wasn't dumped)
assert_equals "$metadata_version" "$(metadata_get_version "${info[master_data_path]}"/metadata.mfs)"

# Restore the filesystem from changelog by starting master server and check it
assert_success lizardfs_master_daemon start
lizardfs_wait_for_ready_chunkservers 4
assert_no_diff "$metadata" "$(metadata_print "${info[mount0]}")"

