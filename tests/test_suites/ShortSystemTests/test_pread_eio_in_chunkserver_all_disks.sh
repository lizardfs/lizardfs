timeout_set 2 minutes

# Create an installation with 3 chunkservers, 3 disks each.
# All disks in CS 0 will fail during the test.
USE_RAMDISK=YES \
	CHUNKSERVERS=3 \
	DISK_PER_CHUNKSERVER=3 \
	CHUNKSERVER_EXTRA_CONFIG="HDD_TEST_FREQ = 0" \
	CHUNKSERVER_0_DISK_0="$RAMDISK_DIR/pread_EIO_hdd_0" \
	CHUNKSERVER_0_DISK_1="$RAMDISK_DIR/pread_EIO_hdd_1" \
	CHUNKSERVER_0_DISK_2="$RAMDISK_DIR/pread_EIO_hdd_2" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|ACCEPTABLE_DIFFERENCE = 1.0`
			`|CHUNKS_WRITE_REP_LIMIT = 20`
			`|REPLICATIONS_DELAY_INIT = 0`
			`|REPLICATIONS_DELAY_DISCONNECT = 0" \
	setup_local_empty_lizardfs info

# Create a directory with many files on mountpoint
cd "${info[mount0]}"
mkdir test
mfssetgoal 2 test
FILE_SIZE=1234 file-generate test/small_{1..30}
FILE_SIZE=300K file-generate test/medium_{1..30}
FILE_SIZE=2M   file-generate test/big_{1..30}

# Restart the first chunkserver preloading pread with EIO-throwing version
LD_PRELOAD="$LIZARDFS_ROOT/lib/libchunk_operations_eio.so" \
		assert_success lizardfs_chunkserver_daemon 0 restart
lizardfs_wait_for_all_ready_chunkservers

# Read 40% of our files, redefined pread is supposed to return EIO.
# Do this many times to make it more probable that the damaged disks will be used.
for i in {1..10}; do
	assert_success file-validate test/*[0124]
done

# Assert that exactly disks marked "pread_EIO" are marked as damaged
list=$(lizardfs_probe_master list-disks)
assert_equals 9 "$(wc -l <<< "$list")"
assert_awk_finds_no '(/EIO/ && $4 != "yes") || (!/EIO/ && $4 == "yes")' "$list"

# Assert that data is replicated to chunkservers 1, 2 and no chunk is stored on cs 0
for f in test/*; do
	assert_eventually_prints "" "mfsfileinfo '$f' | grep ':${info[chunkserver0_port]}'"
	assert_eventually_prints 2 "mfsfileinfo '$f' | grep copy | wc -l"
done
