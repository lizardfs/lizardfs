timeout_set 70 seconds

CHUNKSERVERS=4 \
	USE_RAMDISK=YES \
	MOUNTS=2 \
	MOUNT_EXTRA_CONFIG="cacheexpirationtime=10000"
	setup_local_empty_lizardfs info

repeated_validate() {
	if REPEAT_AFTER_MS=$1 file-validate $2; then
		test_add_failure "Delayed check returned cached values!"
	fi
}

cd ${info[mount0]}

# Create a file
mkdir test

# Validate it on both mounts
FILE_SIZE=32M file-generate test/file
if ! file-validate test/file; then
	test_add_failure "Data read from file is different than written"
fi
if ! file-validate ${info[mount1]}/test/file; then
	test_add_failure "Data read from file is different than written"
fi

# Register delayed validation of test file
repeated_validate 3000 test/file&

# Change the file
dd if=/dev/zero of=test/file bs=256K seek=8 count=4 conv=notrunc oflag=direct

# Ensure that file is now broken, i.e. read cache was flushed after a successful write
if file-validate test/file; then
	test_add_failure "Data read is not up to date"
fi

# Ensure that file is broken on the other mount as well
if file-validate test/file; then
	test_add_failure "Data read is not up to date"
fi

# Ensure that delayed validation also fails, i.e. it did not validate from read cache
wait
