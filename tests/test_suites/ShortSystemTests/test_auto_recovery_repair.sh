timeout_set 2 minutes
CHUNKSERVERS=2 \
	USE_RAMDISK=YES \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota" \
	MASTER_EXTRA_CONFIG="MAGIC_DISABLE_METADATA_DUMPS = 1|AUTO_RECOVERY = 1" \
	setup_local_empty_lizardfs info

# Remember version of the metadata file. We expect it not to change when generating data.
metadata_version=$(metadata_get_version "${info[master_data_path]}"/metadata.mfs)

cd ${info[mount0]}
mkdir dir
mfssetgoal 2 dir
FILE_SIZE=$(( 3 * LIZARDFS_CHUNK_SIZE )) file-generate dir/file

assert_equals 3 $(find_chunkserver_chunks 0 | wc -l)
assert_equals 3 $(find_chunkserver_chunks 1 | wc -l)

# Stop chunkserver0, and remove second chunk from it
lizardfs_chunkserver_daemon 0 stop
chunk=$(find_chunkserver_chunks 0 -name "chunk_0000000000000002_00000001.mfs")
assert_success rm "$chunk"

# Update first chunk of the file, this will change it's version to 2 on CS 1.
dd if=/dev/zero of=dir/file conv=notrunc bs=32KiB count=2
assert_equals 1 $(find_chunkserver_chunks 0 -name "chunk_0000000000000001_00000001.mfs" | wc -l)
assert_equals 1 $(find_chunkserver_chunks 1 -name "chunk_0000000000000001_00000002.mfs" | wc -l)

# Stop chunkserver1, turn on chunkserver 0 again. File should have two chunks lost.
lizardfs_chunkserver_daemon 1 stop
lizardfs_wait_for_ready_chunkservers 0
lizardfs_chunkserver_daemon 0 start
lizardfs_wait_for_ready_chunkservers 1
assert_awk_finds '/chunks with 0 copies: *2$/' "$(mfscheckfile dir/file)"

# Repair the file and remember metadata after the repair
repairinfo=$(mfsfilerepair dir/file)
assert_awk_finds '/chunks not changed: *1$/' "$repairinfo"
assert_awk_finds '/chunks erased: *1$/' "$repairinfo"
assert_awk_finds '/chunks repaired: *1$/' "$repairinfo"
assert_awk_finds_no '/chunks with 0 copies/' "$(mfscheckfile dir/file)"
metadata=$(metadata_print)

# Simulate crash of the master
cd
lizardfs_master_daemon kill

# Make sure changes are in changelog only (ie. that metadata wasn't dumped)
assert_equals "$metadata_version" "$(metadata_get_version "${info[master_data_path]}"/metadata.mfs)"

# Restore the filesystem from changelog by starting master server and check it
assert_success lizardfs_master_daemon start
lizardfs_wait_for_ready_chunkservers 1
assert_no_diff "$metadata" "$(metadata_print "${info[mount0]}")"
