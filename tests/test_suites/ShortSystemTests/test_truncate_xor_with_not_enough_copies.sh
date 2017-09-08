CHUNKSERVERS=4 \
	MASTER_EXTRA_CONFIG="OPERATIONS_DELAY_INIT = 100000" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER,mfsioretries=3" \
	MASTER_CUSTOM_GOALS="10 ec31: \$ec(3,1)" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

mkdir dir
lizardfs setgoal ec31 dir
cd dir

echo "ABCDEFGHIJKLMNOPQRSTUVWXYZ" > file

lizardfs_chunkserver_daemon 0 stop
lizardfs_chunkserver_daemon 1 stop
lizardfs_wait_for_ready_chunkservers 2

# Not enough parts are available
assert_equals "not enough parts available" "$(lizardfs fileinfo file | grep parts | xargs)"
assert_equals 2 $(lizardfs fileinfo file | grep copy | wc -l)

assert_failure dd if=/dev/zero of=file bs=1 count=10 conv=notrunc

assert_failure truncate -s5 file

lizardfs_chunkserver_daemon 0 start
lizardfs_wait_for_ready_chunkservers 3

# There should be enough parts now
assert_equals "" "$(lizardfs fileinfo file | grep parts | xargs)"
assert_equals 3 "$(lizardfs fileinfo file | grep copy | wc -l)"

# Data should be consistent
assert_equals "ABCDEFGHIJKLMNOPQRSTUVWXYZ" $(cat file)

# Truncate should work now
assert_success truncate -s5 file

# File should be writable
echo -n "123" >> file

# Data should be consistent
assert_equals "ABCDE123" $(cat file)
