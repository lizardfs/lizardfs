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
mfssetgoal xor2 .
FILE_SIZE=129M BLOCK_SIZE=12345 file-generate file

file-validate file
assert_file_not_exists $TEMP_DIR/log

cd "${info[mount1]}"/dir
file-validate file
assert_equals \
	"$(grep ^chunkserver.hdd_prefetch_blocks "$TEMP_DIR"/log | sort)" \
	"$(seq 3 | sed "s/\(.*\)/chunkserver.hdd_prefetch_blocks: `
		`chunk:\1 status:0 firstBlock:0 nrOfBlocks:1/")"

