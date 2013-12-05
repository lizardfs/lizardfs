timeout_set 60 seconds

CHUNKSERVERS=2 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

cd ${info[mount0]}
FILE_SIZE=123456789 BLOCK_SIZE=12345 file-generate file
if ! file-validate file; then
        test_add_failure "Data read from file is different than written"
fi
