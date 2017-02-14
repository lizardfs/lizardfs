CHUNKSERVERS=3 \
	USE_RAMDISK="YES" \
	setup_local_empty_lizardfs info

# Create files for the test and move all of them to trash
cd "${info[mount0]}"
mkdir dir
cd dir

chmod 777 .

# Create a file with appropriate permissions
assert_success sudo -nu lizardfstest_0 touch file1
assert_success sudo -nu lizardfstest_0 chmod 600 file1

# Ensure that default user (lizardfstest) obeys access rights
assert_failure cat file1
assert_failure sudo -nu lizardfstest_3 cat file1
assert_failure dd if=/dev/zero bs=4 count=16 of=file1 oflag=direct
assert_failure sudo -nu lizardfstest_3 dd if=/dev/zero bs=4 count=16 of=file1 oflag=direct

# Change group of a file, ensure that it is allowed only for groups that the user belongs to
assert_failure sudo -nu lizardfstest_0 chown -R lizardfstest_0:root file1
assert_success sudo -nu lizardfstest_0 chown -R lizardfstest_0:lizardfstest file1

# Ensure that default user obeys access rights combinations
assert_failure cat file1
assert_failure sudo -nu lizardfstest_3 cat file1
assert_failure dd if=/dev/zero bs=4 count=16 of=file1 oflag=direct
assert_failure sudo -nu lizardfstest_3 dd if=/dev/zero bs=4 count=16 of=file1 oflag=direct

sudo -nu lizardfstest_0 chmod 640 file1

assert_success cat file1
assert_success sudo -nu lizardfstest_3 cat file1
assert_failure dd if=/dev/zero bs=4 count=16 of=file1 oflag=direct
assert_failure sudo -nu lizardfstest_3 dd if=/dev/zero bs=4 count=16 of=file1 oflag=direct

sudo -nu lizardfstest_0 chmod 660 file1

assert_success cat file1
assert_success sudo -nu lizardfstest_3 cat file1
assert_success dd if=/dev/zero bs=4 count=16 of=file1 oflag=direct
assert_success sudo -nu lizardfstest_3 dd if=/dev/zero bs=4 count=16 of=file1 oflag=direct

# Ensure that user cannot steal file ownership even though he has write rights
assert_failure chown -R lizardfstest:lizardfstest file1
assert_failure sudo -nu lizardfstest_3 chown -R lizardfstest:lizardfstest file1
