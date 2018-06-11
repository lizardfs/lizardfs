timeout_set 3 minutes

# Create an installation with 3 chunkservers, 3 disks each.
# All disks in CS 0 will fail during the test.
USE_RAMDISK=YES \
	MOUNTS=2
	CHUNKSERVERS=3 \
	DISK_PER_CHUNKSERVER=3 \
	CHUNKSERVER_EXTRA_CONFIG="HDD_TEST_FREQ = 10000" \
	CHUNKSERVER_0_DISK_0="$RAMDISK_DIR/pread_EIO_hdd_0" \
	CHUNKSERVER_0_DISK_1="$RAMDISK_DIR/pread_EIO_hdd_1" \
	CHUNKSERVER_0_DISK_2="$RAMDISK_DIR/pread_EIO_hdd_2" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|ACCEPTABLE_DIFFERENCE = 1.0`
			`|CHUNKS_WRITE_REP_LIMIT = 50`
			`|CHUNKS_READ_REP_LIMIT = 50`
			`|OPERATIONS_DELAY_INIT = 0`
			`|OPERATIONS_DELAY_DISCONNECT = 0" \
	setup_local_empty_lizardfs info

# Create a directory with many files on mountpoint
cd "${info[mount0]}"
mkdir goal2
lizardfs setgoal 2 goal2
FILE_SIZE=1234 file-generate goal2/small_{1..30}
FILE_SIZE=300K file-generate goal2/medium_{1..30}
FILE_SIZE=2M   file-generate goal2/big_{1..30}

# Restart the first chunkserver preloading pread with EIO-throwing version
LD_PRELOAD="$LIZARDFS_ROOT/lib/libchunk_operations_eio.so" \
		assert_success lizardfs_chunkserver_daemon 0 restart
lizardfs_wait_for_all_ready_chunkservers

# Read 70% of our files, redefined pread is supposed to return EIO.
# Do this many times to make it more probable that the damaged disks will be used.
for i in {1..50}; do
	cd ${info[mount$((i % 2))]}
	assert_success file-validate goal2/*[0124678]
	sleep 0.2
done

# Assert that exactly disks marked "pread_EIO" are marked as damaged
sleep 1
list=$(lizardfs_probe_master list-disks)
assert_equals 9 "$(wc -l <<< "$list")"
assert_awk_finds_no '(/EIO/ && $4 != "yes") || (!/EIO/ && $4 == "yes")' "$list"

# Assert that data is replicated to chunkservers 1, 2 and no chunk is stored on cs 0
for f in goal2/*; do
	assert_eventually_prints "" "lizardfs fileinfo '$f' | grep ':${info[chunkserver0_port]}'" "60 sec"
	assert_eventually_prints 2 "lizardfs fileinfo '$f' | grep copy | wc -l" "60 sec"
done
