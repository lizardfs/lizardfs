CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

cd ${info[mount0]}

block_size=65536
chunk_size=$((1024 * block_size))

for filesize in 90 $((5 * block_size)) $((9 * block_size - 30)) $((chunk_size - 30)) $chunk_size; do
	echo "Testing size $filesize"
	# Generate file
	FILE_SIZE=$filesize file-generate file
	# Add 1000 bytes at the and of the file and remove them using truncate
	cat /dev/urandom | head -c 1000 >> file
	truncate -s $filesize file
	# Check if the file has valid both size and contents
	if ! file-validate file; then
		test_add_failure "Truncate file to $filesize bytes failed"
	fi
	rm file
done
