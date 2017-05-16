timeout_set 60 seconds

CHUNKSERVERS=3 \
	MOUNTS=2 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MOUNT_1_EXTRA_CONFIG="mfsprefetchxorstripes" \
	CHUNKSERVER_EXTRA_CONFIG="MAGIC_DEBUG_LOG = $TEMP_DIR/log|LOG_FLUSH_ON=DEBUG" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
mkdir dir
cd dir
lizardfs setgoal xor2 .
FILE_SIZE=129M BLOCK_SIZE=12345 file-generate file

file-validate file

cd "${info[mount1]}"/dir
file-validate file

# Check if not to many blocks were prefetched (ideally we expect 3
# of them to be prefetched, but in case of timeouts it can be more)
assert_less_or_equal "$(grep ^chunkserver.hdd_prefetch_blocks "$TEMP_DIR"/log | wc -l)" "8"

# Check if at least first blocks of all chunks were prefetched
for i in {1..3}; do
	fetched=($(grep -oP "nrOfBlocks: *\K[0-9]+" "$TEMP_DIR/log"))
	for block in ${fetched[@]}; do
		assert_less_or_equal 1 $block
	done
done
