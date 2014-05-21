test_end
CHUNKSERVERS=4 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

# Create a small file of goal 2
cd "${info[mount0]}"
touch file
mfssetgoal 2 file
FILE_SIZE=1M file-generate file
assert_success file-validate file
assert_equals 2 $(find_all_chunks | wc -l)

# Create some snapshots of this file
mfsmakesnapshot file file_snapshot1
mfsmakesnapshot file file_snapshot2
mfsmakesnapshot file file_snapshot3
assert_success file-validate file*
assert_equals 2 $(find_all_chunks | wc -l)

# Modify the original file and check if data in snapshots remains unchanged
dd if=/dev/zero of=file bs=8KiB count=100 conv=notrunc
assert_success file-validate file_snapshot*
file-overwrite file
assert_success file-validate file*
assert_equals 4 $(find_all_chunks | wc -l)

# Truncate file_snapshot2 up and verify if chunk was duplicated, then fix the data in this file
truncate -s 2M file_snapshot2
assert_equals 6 $(find_all_chunks | wc -l)
file-overwrite file_snapshot2
assert_success file-validate file*
assert_equals 6 $(find_all_chunks | wc -l)

# Truncate file_snapshot3 down and verify if chunk was duplicated, then fix the data in this file
truncate -s 500KiB file_snapshot3
assert_equals 8 $(find_all_chunks | wc -l)
file-overwrite file_snapshot3
assert_success file-validate file*
assert_equals 8 $(find_all_chunks | wc -l)
