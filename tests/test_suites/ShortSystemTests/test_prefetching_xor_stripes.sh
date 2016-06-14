timeout_set 60 seconds

CHUNKSERVERS=3 \
	MOUNTS=2 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	FUSE_EXTRA_CONFIG="max_read=65536" \
	MOUNT_1_EXTRA_CONFIG="mfsprefetchxorstripes" \
	CHUNKSERVER_EXTRA_CONFIG="MAGIC_DEBUG_LOG = chunkserver.hdd_prefetch_blocks:$TEMP_DIR/log" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
mkdir dir
cd dir
lizardfs setgoal xor2 .
FILE_SIZE=129M BLOCK_SIZE=12345 file-generate file

file-validate file
assert_file_not_exists $TEMP_DIR/log

cd "${info[mount1]}"/dir
file-validate file

# Check if not to many blocks were prefetched (ideally we expect 3
# of them to be prefetched, but in case of timeouts it can be more)
assert_less_or_equal "$(grep ^chunkserver.hdd_prefetch_blocks "$TEMP_DIR"/log | wc -l)" "8"

# Check if first blocks of all chunks were prefetched
for i in {1..3}; do
	assert_awk_finds \
		"/^chunkserver.hdd_prefetch_blocks: chunk:$i status:0 firstBlock:0 nrOfBlocks:1$/" \
		"$(cat "$TEMP_DIR"/log)"
done
