USE_RAMDISK=YES \
	CHUNKSERVERS=5 \
	MASTER_CUSTOM_GOALS="10 ec : \$ec(3,2)"\
	setup_local_empty_lizardfs info

cd ${info[mount0]}
mkdir test
lizardfs setgoal ec test

FILE_SIZE=1K file-generate test/file{1..5}

for i in {1..5}; do
	assert_success file-validate test/file$i
done;

for CS in {0..4}; do
	lizardfs_chunkserver_daemon $CS stop
done

lizardfs filerepair -c test/file1

for CS in {0..4}; do
	lizardfs_chunkserver_daemon $CS start
done

lizardfs_wait_for_all_ready_chunkservers

cd ..

# Unmount old LizardFS client 0:
assert_success lizardfs_mount_unmount 0
# Mount LizardFS client 0:
assert_success lizardfs_mount_start 0

cd ${info[mount0]}

for i in {1..5}; do
	assert_success file-validate test/file$i
done;

for CS in {0..4}; do
	lizardfs_chunkserver_daemon $CS stop
done

lizardfs filerepair test/file1

for CS in {0..4}; do
	lizardfs_chunkserver_daemon $CS start
done

lizardfs_wait_for_all_ready_chunkservers

cd ..

# Unmount old LizardFS client 0:
assert_success lizardfs_mount_unmount 0
# Mount LizardFS client 0:
assert_success lizardfs_mount_start 0

cd ${info[mount0]}

assert_failure file-validate test/file1

for i in {2..5}; do
	assert_success file-validate test/file$i
done;
