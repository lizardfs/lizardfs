timeout_set 2 minutes

# Create an installation with 3 chunkservers, 3 disks each.
# All disks in CS 0 will fail during the test.
USE_RAMDISK=YES \
	MOUNTS=1
	CHUNKSERVERS=3 \
	DISK_PER_CHUNKSERVER=3 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER|direct_io|DirectIO"
	setup_local_empty_lizardfs info

# Create a directory with many files on mountpoint
cd "${info[mount0]}"
mkdir goal3
lizardfs setgoal 2 goal3

for file in {1..1000}; do
	FILE_SIZE=1K file-generate goal3/test_${file}
done

lizardfs_chunkserver_daemon 0 stop
lizardfs_chunkserver_daemon 1 stop
lizardfs_chunkserver_daemon 2 stop

lizardfs_chunkserver_daemon 0 start
lizardfs_chunkserver_daemon 1 start
LD_PRELOAD="${LIZARDFS_INSTALL_FULL_LIBDIR}/libslow_chunk_scan.so" lizardfs_chunkserver_daemon 2 start

lizardfs_wait_for_all_ready_chunkservers

sleep 5

# if this timeouts then there is a bug
for file in {1..100}; do
	FILE_SIZE=1K file-generate goal3/test_valid_${file}
	file-validate goal3/test_valid_${file}
done
