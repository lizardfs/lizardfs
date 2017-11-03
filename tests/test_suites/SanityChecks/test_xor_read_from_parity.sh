CHUNKSERVERS=4 \
	DISK_PER_CHUNKSERVER=1 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

dir="${info[mount0]}/dir"
mkdir "$dir"
lizardfs setgoal xor3 "$dir"
FILE_SIZE=6M file-generate "$dir/file"

# Find the chunkserver serving part 1 of 2 and stop it
csid=$(find_first_chunkserver_with_chunks_matching 'chunk_xor_1_of_3*')
lizardfs_chunkserver_daemon $csid stop

if ! file-validate "$dir/file"; then
	test_add_failure "Data read from file is different than written"
fi
