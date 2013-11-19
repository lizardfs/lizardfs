timeout_set 6 minutes

CHUNKSERVERS=3 \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

# file size in MB
file_size_mb=1000
test_filename=speed_test_file

# write file and measure time
time_file=$TEMP_DIR/$(unique_file)
/usr/bin/time -o $time_file -f %e dd \
	if=/dev/zero \
	of=${test_filename} \
	bs=128K \
	count=$((file_size_mb * 8)) \
	conv=fsync

write_time=$(<$time_file)
write_speed=$(echo "scale=3;${file_size_mb}/${write_time}" | bc)

drop_caches

# read file measuring time
/usr/bin/time -o $time_file -f %e dd \
	if=${test_filename} \
	of=/dev/null bs=64K

read_time=$(<$time_file)
read_speed=$(echo "scale=3;${file_size_mb}/${read_time}" | bc)

# write test results to csv file
echo -e "Write speed,Read speed\n${write_speed}, ${read_speed}" | tee \
	$TEST_OUTPUT_DIR/disk_speed_results.csv
