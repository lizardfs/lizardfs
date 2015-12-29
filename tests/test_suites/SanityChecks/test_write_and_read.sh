timeout_set 70 seconds

CHUNKSERVERS=23 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_EXTRA_CONFIG="READ_AHEAD_KB = 1024|MAX_READ_BEHIND_KB = 2048" \
	MASTER_CUSTOM_GOALS="8 ec_4_17: \$ec(4,17)"
	setup_local_empty_lizardfs info

cd ${info[mount0]}

mkdir dir_std
FILE_SIZE=123456789 BLOCK_SIZE=12345 file-generate dir_std/file
if ! file-validate dir_std/file; then
	test_add_failure "Data read from file is different than written"
fi

mkdir dir_xor
mfssetgoal -r xor2 dir_xor
FILE_SIZE=123456789 BLOCK_SIZE=12345 file-generate dir_xor/file
if ! file-validate dir_xor/file; then
	test_add_failure "Data read from file is different than written"
fi

mkdir dir_ec
mfssetgoal -r ec32 dir_ec
FILE_SIZE=123456789 BLOCK_SIZE=12345 file-generate dir_ec/file
if ! file-validate dir_ec/file; then
	test_add_failure "Data read from file is different than written"
fi

mkdir dir_turbo_ec
mfssetgoal -r ec_4_17 dir_turbo_ec
FILE_SIZE=123456789 BLOCK_SIZE=12345 file-generate dir_turbo_ec/file
if ! file-validate dir_turbo_ec/file; then
	test_add_failure "Data read from file is different than written"
fi
