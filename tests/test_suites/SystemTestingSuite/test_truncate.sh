CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

cd ${info[mount0]}

block_size=$LIZARDFS_BLOCK_SIZE
chunk_size=$LIZARDFS_CHUNK_SIZE
first_loop=yes

for goal in 2 xor2; do
	if [[ $first_loop == no ]]; then
		# Empty the ramdisk to prevent running out of space in case of big chunks
		find_all_chunks | xargs rm -f
		mfschunkserver -c "${info[chunkserver0_config]}" restart &
		mfschunkserver -c "${info[chunkserver1_config]}" restart &
		mfschunkserver -c "${info[chunkserver2_config]}" restart &
		wait
	else
		first_loop=no
	fi
	for filesize in 90 $((5 * block_size)) $((9 * block_size - 30)) $((chunk_size - 30)) \
			$((chunk_size + 30)); do
		echo "Testing size $filesize goal $goal"
		mkdir -p tmp;
		mfssetgoal $goal tmp
		# Generate file
		FILE_SIZE=$filesize file-generate tmp/file
		# Add 1000 bytes at the and of the file and remove them using truncate
		cat /dev/urandom | head -c 1000 >> tmp/file
		truncate -s $filesize tmp/file
		# Check if the file has valid both size and contents
		if ! file-validate tmp/file; then
			test_add_failure "Truncate file to $filesize bytes failed"
		fi
		# Check if it is still possible to append data to a truncated file and read it
		head -c 1000 /dev/urandom >> tmp/file
		tail -c 1050 tmp/file > /dev/null
		rm tmp/file
	done
done
