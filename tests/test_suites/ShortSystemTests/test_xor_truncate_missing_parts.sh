timeout_set '3 minutes'

CHUNKSERVERS=10 \
	DISK_PER_CHUNKSERVER=1 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

# List of file sizes which will be tested for each xor level
sizes=(100 200 300 1000 2000 3000 10000 20000 30000 100000 200000 300000 \
	$((LIZARDFS_BLOCK_SIZE - 1)) $((8 * LIZARDFS_BLOCK_SIZE)) $((50 * LIZARDFS_BLOCK_SIZE + 7)) \
	$((LIZARDFS_CHUNK_SIZE - 500)) $((LIZARDFS_CHUNK_SIZE + 500)) \
)

# List of xor levels which will be tested
levels=(2 3 4 7 9)

# For each xor level and each file size generate file of this size (using file-generate) and
# append some random amount of random bytes to it. Then make snapshot of such a file.
pseudorandom_init
cd "${info[mount0]}"
for i in "${levels[@]}"; do
	mkdir xor$i
	lizardfs setgoal xor$i xor$i
	for size in "${sizes[@]}"; do
		FILE_SIZE=$size file-generate xor$i/file_$size
		assert_success file-validate xor$i/file_$size
		head -c $(pseudorandom 1 $((i * 100000))) /dev/urandom >> xor$i/file_$size
		lizardfs makesnapshot xor$i/file_$size xor$i/snapshot_$size
	done
done

# Now remove one of chunkservers
lizardfs_chunkserver_daemon 0 stop

# For each created file restore its original size using truncate (ie. chop off the random bytes
# appended after generating the file) and verify if the data is OK.
for i in "${levels[@]}"; do
	for size in "${sizes[@]}"; do
		MESSAGE="Truncating xor$i from $(stat -c %s xor$i/file_$size) bytes to $size bytes"
		assert_success truncate -s $size xor$i/snapshot_$size
		assert_success truncate -s $size xor$i/file_$size
		assert_success file-validate xor$i/file_$size xor$i/snapshot_$size
	done
done

# Verify again after starting the chunkserver
lizardfs_chunkserver_daemon 0 start
lizardfs_wait_for_ready_chunkservers 10
MESSAGE="Verification after starting the chunkserver" assert_success file-validate xor*/*
