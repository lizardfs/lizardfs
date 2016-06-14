timeout_set 70 seconds

CHUNKSERVERS=6 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_EXTRA_CONFIG="READ_AHEAD_KB = 1024|MAX_READ_BEHIND_KB = 2048" \
	MASTER_CUSTOM_GOALS="8 ec_4_17: \$ec(4,17)|9 ec_6_10: \$ec(6,10)"
	setup_local_empty_lizardfs info

cd ${info[mount0]}

mkdir dir_ec_4_17
lizardfs setgoal -r ec_4_17 dir_ec_4_17
FILE_SIZE=123456789 BLOCK_SIZE=12345 file-generate dir_ec_4_17/file
if ! file-validate dir_ec_4_17/file; then
	test_add_failure "Data read from file is different than written"
fi
for part in {1..4}; do
	assert_awk_finds "/part $part\/21/" "$(lizardfs fileinfo dir_ec_4_17/file)"
done

mkdir dir_ec_6_10
lizardfs setgoal -r ec_6_10 dir_ec_6_10
FILE_SIZE=123456789 BLOCK_SIZE=12345 file-generate dir_ec_6_10/file
if ! file-validate dir_ec_6_10/file; then
	test_add_failure "Data read from file is different than written"
fi
for part in {1..6}; do
	assert_awk_finds "/part $part\/16/" "$(lizardfs fileinfo dir_ec_6_10/file)"
done

# Remove 3 chunkservers in order to leave space only for xor3 parity
for i in {3..5}; do
	lizardfs_chunkserver_daemon $i stop
done

mkdir dir_xor3
lizardfs setgoal -r xor3 dir_xor3
FILE_SIZE=123456789 BLOCK_SIZE=12345 file-generate dir_xor3/file
if ! file-validate dir_xor3/file; then
	test_add_failure "Data read from file is different than written"
fi
# Xors have parity parts registered as 'part 1'
for part in {2..4}; do
	assert_awk_finds "/part $part\/4/" "$(lizardfs fileinfo dir_xor3/file)"
done
