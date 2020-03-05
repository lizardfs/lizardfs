timeout_set '45 seconds'

# Create an installation with 2 chunkservers, 3 disks each.
# Two out of three disks in CS 0 will fail during the test.
USE_RAMDISK=YES \
	CHUNKSERVERS=2 \
	DISK_PER_CHUNKSERVER=3 \
	CHUNKSERVER_EXTRA_CONFIG="HDD_TEST_FREQ = 10000" \
	CHUNKSERVER_0_DISK_0="$RAMDISK_DIR/pread_EIO_hdd_0" \
	CHUNKSERVER_0_DISK_1="$RAMDISK_DIR/pread_EIO_hdd_1" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_MIN_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|ACCEPTABLE_DIFFERENCE = 1.0`
			`|CHUNKS_WRITE_REP_LIMIT = 30`
			`|OPERATIONS_DELAY_INIT = 0`
			`|OPERATIONS_DELAY_DISCONNECT = 0" \
	setup_local_empty_lizardfs info

# Create a directory with many files on mountpoint.
cd "${info[mount0]}"
mkdir goal1
mkdir goal2
lizardfs setgoal 1 goal1
lizardfs setgoal 2 goal2

FILE_SIZE=1234 file-generate goal{1..2}/small_{1..10}
FILE_SIZE=300K file-generate goal{1..2}/medium_{1..10}
FILE_SIZE=5M   file-generate goal{1..2}/big_{1..10}

# Count chunks in the system. Expect that each chunk has one copy on CS0 and one copy on CS1.
chunks=$(lizardfs_probe_master info | awk '{print $12}')
assert_equals 60 $chunks
assert_equals 90 $(lizardfs_probe_master info | awk '{print $13}')
assert_less_than 30 "$(find_chunkserver_chunks 0 | wc -l)"
assert_less_than 30 "$(find_chunkserver_chunks 1 | wc -l)"

# Restart the first chunkserver preloading pread with EIO-throwing version
LD_PRELOAD="$LIZARDFS_ROOT/lib/libchunk_operations_eio.so" \
		assert_success lizardfs_chunkserver_daemon 0 restart
lizardfs_wait_for_all_ready_chunkservers

# Read our files, redefined pread is supposed to return EIO.
# Do this many times to make it more probable that the damaged disks will be used.
for i in {1..20}; do
	assert_success file-validate goal2/*
done
for file in goal1/*; do
	file-validate $file &
done

# Assert that exactly disks marked "pread_EIO" are marked as damaged.
sleep 1
list=$(lizardfs_probe_master list-disks)
assert_equals 6 "$(wc -l <<< "$list")"
assert_awk_finds_no '(/EIO/ && $4 != "yes") || (!/EIO/ && $4 == "yes")' "$list"

# Remove files with goal 1 and all their chunks
for file in $(find_chunkserver_chunks 0 | grep -v EIO); do
	chunkid=$(cut -d'/' -f6 <<< "$file" | cut -d'_' -f2)
	if lizardfs fileinfo goal1/* | grep "$chunkid"; then
		rm $file
	fi
done
rm -r goal1

# Verify if all chunks will be eventually replicated to the working disk on CS0
assert_eventually_prints 30 'find_chunkserver_chunks 0 | grep -v EIO | wc -l'
