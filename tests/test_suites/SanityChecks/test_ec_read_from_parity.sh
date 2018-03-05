CHUNKSERVERS=4 \
	DISK_PER_CHUNKSERVER=1 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

dir="${info[mount0]}/dir"
mkdir "$dir"
lizardfs setgoal ec22 "$dir"
FILE_SIZE=6M file-generate "$dir/file"

# Find the chunkserver serving part 1 of 4 and stop it
csid=$(find_first_chunkserver_with_chunks_matching 'chunk_ec2_1_of_*')
lizardfs_chunkserver_daemon $csid stop

# Find the chunkserver serving part 2 of 4 and stop it
csid=$(find_first_chunkserver_with_chunks_matching 'chunk_ec2_2_of_*')
lizardfs_chunkserver_daemon $csid stop

if ! file-validate "$dir/file"; then
	test_add_failure "Data read from file is different than written"
fi
