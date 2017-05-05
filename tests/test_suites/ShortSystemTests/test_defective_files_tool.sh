timeout_set 60 seconds

CHUNKSERVERS=5 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER"
	MASTER_EXTRA_CONFIG="OPERATIONS_DELAY_INIT = 0|FILE_TEST_LOOP_MIN_TIME = 1|OPERATIONS_DELAY_DISCONNECT = 0" \
	setup_local_empty_lizardfs info

cd ${info[mount0]}

mkdir dir_ec
lizardfs setgoal -r ec32 dir_ec
FILE_SIZE=$((3*64*1024)) BLOCK_SIZE=1024 file-generate dir_ec/file

lizardfs_chunkserver_daemon 0 stop

assert_eventually_prints 1 "lizardfs_admin_master list-defective-files --undergoal --porcelain | wc -l"
assert_equals 0 $(lizardfs_admin_master list-defective-files --unavailable --porcelain | wc -l)
assert_equals 0 $(lizardfs_admin_master list-defective-files --structure-error --porcelain | wc -l)

for CS in {1..2}; do
	lizardfs_chunkserver_daemon $CS stop
done

assert_eventually_prints 1 "lizardfs_admin_master list-defective-files --unavailable --porcelain | wc -l"
assert_equals 0 $(lizardfs_admin_master list-defective-files --undergoal --porcelain | wc -l)
assert_equals 0 $(lizardfs_admin_master list-defective-files --structure-error --porcelain | wc -l)

for CS in {0..2}; do
	lizardfs_chunkserver_daemon $CS start
done

assert_eventually_prints 0 "lizardfs_admin_master list-defective-files --unavailable --porcelain | wc -l"
assert_equals 0 $(lizardfs_admin_master list-defective-files --structure-error --porcelain | wc -l)
