timeout_set 30 minutes

CHUNKSERVERS=3 \
	setup_local_empty_lizardfs info

file_size_mb=1000
cd "${info[mount0]}"

for goal in 1 2 xor2 xor3; do
	test_filename=speed_test_file_${goal}
	touch "$test_filename"
	mfssetgoal $goal "$test_filename"

	# write file and measure time
	drop_caches
	time_file=$TEMP_DIR/$(unique_file)
		/usr/bin/time -o "$time_file" -f %e dd \
		if=/dev/zero \
		of="$test_filename" \
		bs=128K \
		count=$((file_size_mb * 8)) \
		conv=fsync
	write_time=$(cat "$time_file")
	write_speed=$(echo "scale=3;${file_size_mb}/${write_time}" | bc)

	# read file and measure time
	drop_caches
	/usr/bin/time -o "$time_file" -f %e dd \
		if="$test_filename" \
		of=/dev/null \
		bs=64K
	read_time=$(cat "$time_file")
	read_speed=$(echo "scale=3;${file_size_mb}/${read_time}" | bc)

	# write test results to a csv file
	echo -e "Write speed (goal $goal),Read speed (goal $goal)\n${write_speed}, ${read_speed}" \
			| tee "${TEST_OUTPUT_DIR}/disk_speed_results_goal_${goal}.csv"
done
