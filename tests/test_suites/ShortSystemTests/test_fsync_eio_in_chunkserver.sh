# Create an installation with 3 chunkservers, 1 disk each.
# CS 0 has a disk which will fail during the test when writing any chunks.
USE_RAMDISK=YES \
	CHUNKSERVERS=3 \
	CHUNKSERVER_EXTRA_CONFIG="HDD_TEST_FREQ = 10000" \
	CHUNKSERVER_0_DISK_0="$RAMDISK_DIR/fsync_EIO_hdd_0" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|ACCEPTABLE_DIFFERENCE = 1.0`
			`|CHUNKS_WRITE_REP_LIMIT = 20`
			`|OPERATIONS_DELAY_INIT = 0`
			`|OPERATIONS_DELAY_DISCONNECT = 0" \
	setup_local_empty_lizardfs info

# Restart the first chunkserver preloading pwrite with EIO-throwing version
LD_PRELOAD="$LIZARDFS_ROOT/lib/libchunk_operations_eio.so" \
		assert_success lizardfs_chunkserver_daemon 0 restart
lizardfs_wait_for_all_ready_chunkservers

# Create a directory with many small files on mountpoint. This should trigger a failure on CS0.
cd "${info[mount0]}"
mkdir test
lizardfs setgoal 2 test
FILE_SIZE=1234 assert_success file-generate test/small_{1..10}
FILE_SIZE=1M   assert_success file-generate test/big_{1..10}

# Wait for the failure to be detected
assert_eventually_prints yes "lizardfs_probe_master list-disks | awk '/EIO/ {print \$4}'"

# Verify that exactly disks marked "fsync_EIO" are marked as damaged
list=$(lizardfs_probe_master list-disks)
assert_equals 3 "$(wc -l <<< "$list")"
assert_awk_finds_no '(/EIO/ && $4 != "yes") || (!/EIO/ && $4 != "no")' "$list"

# Assert that data is replicated to chunkservers 1, 2 and no chunk is stored on cs 0
for f in test/*; do
	assert_eventually_prints "" "lizardfs fileinfo '$f' | grep ':${info[chunkserver0_port]}'"
	assert_eventually_prints 2 "lizardfs fileinfo '$f' | grep copy | wc -l"
done
