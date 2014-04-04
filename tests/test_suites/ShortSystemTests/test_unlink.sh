timeout_set 4 minutes

CHUNKSERVERS=3 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1|REPLICATIONS_DELAY_INIT = 0" \
	USE_RAMDISK="YES" \
	setup_local_empty_lizardfs info
	
# Create a file consising of a couple of chunks and remove it
file="${info[mount0]}/file"
touch "$file"
mfssetgoal 3 "$file"
dd if=/dev/zero of="$file" bs=1MiB count=130
mfssettrashtime 0 "$file"
rm -f "$file"

# Wait for removing all the chunks
hdds=$(cat "${info[chunkserver0_hdd]}" "${info[chunkserver1_hdd]}" "${info[chunkserver2_hdd]}")
timeout="3 minutes"
end_time=$(date +%s -d "$timeout")
echo "Waiting up to $timeout until chunks are removed..."
while (( $(date +%s) < end_time )); do
	chunk_count=$(find $hdds -name 'chunk*' | wc -l)
	if (( chunk_count == 0 )); then
		test_end
	fi
	sleep 1
done
test_add_failure "$chunk_count chunks were not removed within $timeout"
