timeout_set 60 seconds

CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_EXTRA_CONFIG="READ_AHEAD_KB = 1024|MAX_READ_BEHIND_KB = 2048" \
	setup_local_empty_lizardfs info

cd ${info[mount0]}
mkdir dir
cd dir
mfssetgoal xor2 .
FILE_SIZE=123456789 BLOCK_SIZE=12345 file-generate file
if ! file-validate file; then
	test_add_failure "Data read from file is different than written"
fi
