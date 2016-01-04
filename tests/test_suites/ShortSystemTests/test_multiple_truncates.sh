timeout_set 2 minute
CHUNKSERVERS=4 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
block=$LIZARDFS_BLOCK_SIZE
chunk=$LIZARDFS_CHUNK_SIZE
goals="1 xor2 xor3 ec21 ec22"

# Create the same files on LizardFS (in many goals) and in a temporary directory.
# The test will do the same operations (truncate, write) on all the files and compare
# them after each operation.
real_file="$TEMP_DIR/file"
FILE_SIZE=$chunk file-generate "$real_file"
for goal in $goals; do
	touch "file_$goal"
	lizardfs setgoal "$goal" "file_$goal"
	FILE_SIZE=$chunk file-generate "file_$goal"
done

# 'verify_truncate [+-]<size>' truncates all the files and verifies if they are the same
verify_truncate() {
	local size=$1

	export MESSAGE="Veryfing truncate -s $size"
	assert_success truncate -s "$size" "$real_file"
	for goal in $goals; do
		assert_success truncate -s "$size" "file_$goal"
		assert_files_equal "$real_file" "file_$goal"
	done
}

# 'verify_append <size> appends <size> bytes to all the files and verifies if they are the same
verify_append() {
	local size=$1
	local blob=$(base64 --wrap=0 /dev/urandom | head -c "$size")

	export MESSAGE="Appending blob of size $size"
	echo -n "$blob" >> "$real_file"
	for goal in $goals; do
		echo -n "$blob" >> "file_$goal"
		assert_files_equal "$real_file" "file_$goal"
	done
}

# The test scenario -- it makes the files bigger and smaller many times
verify_truncate $((chunk + block))
verify_truncate $((chunk + 2 * block))
verify_append 5000
for i in {1..6}; do verify_truncate -100$i; done
verify_truncate $((chunk + block))
verify_truncate +1000
verify_truncate +1000
verify_append 1000
verify_truncate $((chunk + 100))
verify_truncate $((chunk - 100))
verify_truncate $((chunk - 200 * block))
verify_truncate $((chunk - 300 * block))
verify_truncate $((chunk - 150 * block + 1000))
verify_append 10000
verify_truncate -100
verify_truncate -100
verify_truncate -10000
for i in {0..3}; do lizardfs_chunkserver_daemon "$i" restart & done ; wait
verify_truncate $((chunk + block))
verify_append 1000
verify_truncate $((chunk + 2 * block - 500))
verify_append 1000
verify_truncate $((chunk + 3 * block - 500))
verify_truncate -100
verify_truncate -100
verify_append 1000
verify_truncate +1000
verify_truncate +1000
for i in {0..3}; do lizardfs_chunkserver_daemon "$i" restart & done ; wait
verify_append 1000
