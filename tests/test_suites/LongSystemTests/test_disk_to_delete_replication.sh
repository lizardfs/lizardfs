timeout_set 2 minutes

# Create an installation with 4 chunkservers, 3 disks each.
USE_RAMDISK=YES \
	CHUNKSERVERS=4 \
	DISK_PER_CHUNKSERVER=3 \
	CHUNKSERVER_EXTRA_CONFIG="HDD_TEST_FREQ = 10000" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|ACCEPTABLE_DIFFERENCE = 1.0`
			`|CHUNKS_WRITE_REP_LIMIT = 20`
			`|OPERATIONS_DELAY_INIT = 0`
			`|OPERATIONS_DELAY_DISCONNECT = 0" \
	setup_local_empty_lizardfs info

# This function prints all disks of given chunkserver
# $1 - id of chunkserver
# $2, $3, ..., $n - list of disks that should be marked as to delete in ascending order
print_disks() {
	cs=$1
	shift
	for disk in {0..2}; do
		path="$RAMDISK_DIR/hdd_${cs}_${disk}"
		if [ $# -gt 0 ] && [ $1 == $disk ]; then
			echo *$path
			shift
		else
			echo $path
		fi
	done
}

# This function marks specified disks of given chunkserver as to delete
# $1 - id of chunkserver
# $2, $3, ..., $n - list of disks that should be marked as to delete in ascending order
mark_disks() {
	cs=$1
	print_disks $* > "${info[chunkserver${cs}_hdd]}"
	echo "Hdd config for chunkserver $1 changed."
	cat "${info[chunkserver${cs}_hdd]}"
}

# Stop one of chunkservers
lizardfs_chunkserver_daemon 3 stop

# Create 3 files with one chunk, 3 parts each
cd "${info[mount0]}"
for goal in 3 xor2; do
	dir=dir_$goal
	mkdir $dir
	lizardfs setgoal $goal $dir
	for size in 10K 15K 50K 70K 100K 200K; do
		FILE_SIZE=$size file-generate $dir/file_$size
	done
done

# Check if there are at least 2 chunks on each disk
for cs in {0..2}; do
	for disk in {0..2}; do
		assert_less_or_equal 2 "$(ls $RAMDISK_DIR/hdd_${cs}_${disk}/*/* | wc -l)"
	done
done

# Mark one disk as to delete and reload config
mark_disks 0 1
lizardfs_chunkserver_daemon 0 reload

# Start last chunkserver
lizardfs_chunkserver_daemon 3 start

# Check if files from marked disk will be replicated and deleted
assert_eventually_prints 0 "ls $RAMDISK_DIR/hdd_0_1/*/* | wc -l" "30 seconds"

# Check number of chunks for each file
for file in */*; do
	assert_equals 3 "$(lizardfs fileinfo $file | grep copy | wc -l)"
done

# Unmark disk
mark_disks 0

# Mark all disks from some other chunkserver
mark_disks 1 {0..2}

# Reload config of both chunkservers
lizardfs_chunkserver_daemon 0 reload
lizardfs_chunkserver_daemon 1 reload

# Check if all chunks were deleted from server 1
assert_eventually_prints 0 "find_chunkserver_chunks 1 | wc -l" "60 seconds"

# Check number of chunks for each file
for file in */*; do
	assert_equals 3 "$(lizardfs fileinfo $file | grep copy | wc -l)"
done

