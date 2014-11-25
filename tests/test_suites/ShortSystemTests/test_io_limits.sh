timeout_set 2 minutes

# Create a config file with a limit of 1 MB/s for all processes
iolimits="$TEMP_DIR/iolimits.cfg"
echo "limit unclassified 1024" > "$iolimits"

CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="lfscachemode=NEVER|lfsiolimits=$iolimits" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

time=$(which time) # We need /usr/bin/time or something like this in this test, not a bash built-in
head -c 1M /dev/zero > warmup
for mb in 9 5 3 1; do
	export FILE_SIZE="${mb}M"
	expected_time_ms=$((mb * 1000))

	export MESSAGE="Writing $mb MB at 1 MB/s"
	echo "$MESSAGE"
	seconds=$("$time" -f %e file-generate file_${mb} 2>&1)
	actual_time_ms=$(bc <<< "scale=0; $seconds * 1000 / 1")
	assert_near $expected_time_ms $actual_time_ms 250

	export MESSAGE="Reading $mb MB at 1 MB/s"
	echo "$MESSAGE"
	seconds=$("$time" -f %e file-validate file_${mb} 2>&1)
	actual_time_ms=$(bc <<< "scale=0; $seconds * 1000 / 1")
	assert_near $expected_time_ms $actual_time_ms 250

	export MESSAGE="Reading + writing $mb MB at 1 MB/s"
	echo "$MESSAGE"
	seconds=$("$time" -f %e bash -c "file-validate file_${mb} & file-generate garbage & wait" 2>&1)
	actual_time_ms=$(bc <<< "scale=0; $seconds * 1000 / 1")
	assert_near $((2 * expected_time_ms)) $actual_time_ms 250
done
