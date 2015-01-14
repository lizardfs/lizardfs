# Create an installation with 2 chunkservers, 3 disks each.
# Two out of three disks in CS 0 will fail during the test.
USE_RAMDISK=YES \
	CHUNKSERVERS=2 \
	DISK_PER_CHUNKSERVER=3 \
	CHUNKSERVER_EXTRA_CONFIG="HDD_TEST_FREQ = 0" \
	CHUNKSERVER_0_DISK_0="$RAMDISK_DIR/pread_EIO_hdd_0" \
	CHUNKSERVER_0_DISK_1="$RAMDISK_DIR/pread_EIO_hdd_1" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|ACCEPTABLE_DIFFERENCE = 1.0`
			`|CHUNKS_WRITE_REP_LIMIT = 20`
			`|REPLICATIONS_DELAY_INIT = 0`
			`|REPLICATIONS_DELAY_DISCONNECT = 0" \
	setup_local_empty_lizardfs info

# Create a directory with many files on mountpoint.
cd "${info[mount0]}"
mkdir test
mfssetgoal 2 test
FILE_SIZE=1234 file-generate test/small_{1..10}
FILE_SIZE=300K file-generate test/medium_{1..10}
FILE_SIZE=5M   file-generate test/big_{1..10}

# Count chunks in the system. Expect that each chunk has ony copy on CS0 and one copy on CS2.
chunks=$(lizardfs_probe_master info | awk '{print $12}')
assert_equals "$chunks" "$(find_chunkserver_chunks 0 | wc -l)"
assert_equals "$chunks" "$(find_chunkserver_chunks 1 | wc -l)"

# Restart the first chunkserver preloading pread with EIO-throwing version
LD_PRELOAD="$LIZARDFS_ROOT/lib/libchunk_operations_eio.so" \
		assert_success lizardfs_chunkserver_daemon 0 restart
lizardfs_wait_for_all_ready_chunkservers

# Read 30% of our files, redefined pread is supposed to return EIO.
# Do this many times to make it more probable that the damaged disks will be used.
for i in {1..10}; do
	assert_success file-validate test/*[012]
done

# Assert that exactly disks marked "pread_EIO" are marked as damaged.
list=$(lizardfs_probe_master list-disks)
assert_equals 6 "$(wc -l <<< "$list")"
assert_awk_finds_no '(/EIO/ && $4 != "yes") || (!/EIO/ && $4 == "yes")' "$list"

# Verify if all chunks will be eventually replicated to the working disk on CS0
assert_eventually_prints "$chunks" 'find_chunkserver_chunks 0 | grep -v EIO | wc -l'
