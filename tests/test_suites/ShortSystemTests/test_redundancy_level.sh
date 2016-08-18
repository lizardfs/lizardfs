timeout_set 1 minute

USE_RAMDISK=YES \
	CHUNKSERVERS=5 \
	MASTER_CUSTOM_GOALS="10 ec : \$ec(3,2)`
			  `| 11 xor4 : \$xor4`
			  `| 12 5 : _ _ _ _ _"\
	MASTER_EXTRA_CONFIG="REDUNDANCY_LEVEL = 4"
	setup_local_empty_lizardfs info

lizardfs_chunkserver_daemon 0 stop

cd ${info[mount0]}
mkdir ec_dir xor4_dir std5_dir
lizardfs setgoal ec ec_dir
lizardfs setgoal xor4 xor4_dir
lizardfs setgoal 5 std5_dir

FILE_SIZE=1K assert_success file-generate ec_dir/file
FILE_SIZE=1K assert_success file-generate xor4_dir/file
FILE_SIZE=1K assert_failure file-generate std5_dir/file

sed -ie 's/REDUNDANCY_LEVEL = 4/REDUNDANCY_LEVEL = 2/' "${info[master_cfg]}"
lizardfs_master_daemon reload

FILE_SIZE=1K assert_failure file-generate ec_dir/file1
FILE_SIZE=1K assert_success file-generate xor4_dir/file1
FILE_SIZE=1K assert_success file-generate std5_dir/file1

sed -ie 's/REDUNDANCY_LEVEL = 2/REDUNDANCY_LEVEL = 1/' "${info[master_cfg]}"
lizardfs_master_daemon reload

FILE_SIZE=1K assert_success file-generate ec_dir/file2
FILE_SIZE=1K assert_failure file-generate xor4_dir/file2
FILE_SIZE=1K assert_success file-generate std5_dir/file2

lizardfs_chunkserver_daemon 0 start
lizardfs_wait_for_all_ready_chunkservers

sed -ie 's/REDUNDANCY_LEVEL = 1/REDUNDANCY_LEVEL = 4/' "${info[master_cfg]}"
lizardfs_master_daemon reload

FILE_SIZE=1K assert_success file-generate ec_dir/file3
FILE_SIZE=1K assert_success file-generate xor4_dir/file3
FILE_SIZE=1K assert_success file-generate std5_dir/file3

lizardfs_chunkserver_daemon 0 stop
sed -ie 's/REDUNDANCY_LEVEL = 4/REDUNDANCY_LEVEL = 1/' "${info[master_cfg]}"
lizardfs_master_daemon reload

assert_success dd if=/dev/zero of=ec_dir/file3 bs=4KiB count=1 conv=notrunc
assert_success dd if=/dev/zero of=std5_dir/file3 bs=4KiB count=1 conv=notrunc
assert_failure dd if=/dev/zero of=xor4_dir/file3 bs=4KiB count=1 conv=notrunc

sed -ie 's/REDUNDANCY_LEVEL = 1/REDUNDANCY_LEVEL = 2/' "${info[master_cfg]}"
lizardfs_master_daemon reload

assert_failure dd if=/dev/zero of=ec_dir/file3 bs=4KiB count=1 conv=notrunc
assert_success dd if=/dev/zero of=std5_dir/file3 bs=4KiB count=1 conv=notrunc
assert_success dd if=/dev/zero of=xor4_dir/file3 bs=4KiB count=1 conv=notrunc

sed -ie 's/REDUNDANCY_LEVEL = 2/REDUNDANCY_LEVEL = 4/' "${info[master_cfg]}"
lizardfs_master_daemon reload

assert_success dd if=/dev/zero of=ec_dir/file3 bs=4KiB count=1 conv=notrunc
assert_failure dd if=/dev/zero of=std5_dir/file3 bs=4KiB count=1 conv=notrunc
assert_success dd if=/dev/zero of=xor4_dir/file3 bs=4KiB count=1 conv=notrunc
