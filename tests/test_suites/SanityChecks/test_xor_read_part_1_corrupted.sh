timeout_set 45 seconds

CHUNKSERVERS=4 \
	DISK_PER_CHUNKSERVER=1 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

dir="${info[mount0]}/dir"
mkdir "$dir"
lizardfs setgoal xor3 "$dir"
FILE_SIZE=6M file-generate "$dir/file"

# Corrupt data in part 1 of the chunk
csid=$(find_first_chunkserver_with_chunks_matching 'chunk_xor_1_of_3*')
hdd=$(cat "${info[chunkserver${csid}_hdd]}")
chunk=$(find "$hdd" -name 'chunk_xor_1_of_3_*.???')
echo aaaa | dd of="$chunk" bs=1 count=4 seek=6k conv=notrunc

if ! file-validate "$dir/file"; then
	test_add_failure "Data read from file is different than written"
fi
