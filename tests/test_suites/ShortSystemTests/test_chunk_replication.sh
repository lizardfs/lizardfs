: ${test_timeout:="5 minutes"}
: ${replication_timeout:="90 seconds"}
: ${number_of_chunkservers:=12}
: ${goals="2 3 4 5 6 7 8 9 xor2 xor3 xor4 xor5 xor6 xor7 xor8 xor9"}
: ${verify_file_content=YES}

# Returns list of all chunks in the following format:
# chunk 0000000000000001_00000001 parity 6
# chunk 0000000000000001_00000001 part 1/6
# chunk 0000000000000003_00000001
# chunk 0000000000000004_00000001 part 1/3
get_list_of_chunks() {
	lizardfs fileinfo */* | awk '/\tchunk/{id=$3} /\tcopy/{print "chunk",id,$4,$5}' | sort
}

timeout_set "$test_timeout"
CHUNKSERVERS=$number_of_chunkservers \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|CHUNKS_WRITE_REP_LIMIT = 10`
			`|OPERATIONS_DELAY_INIT = 0`
			`|OPERATIONS_DELAY_DISCONNECT = 0`
			`|ACCEPTABLE_DIFFERENCE = 10" \
	setup_local_empty_lizardfs info

# Create files with goals from the $goals list
cd "${info[mount0]}"
for goal in $goals; do
	dir="dir_$goal"
	mkdir "$dir"
	lizardfs setgoal "$goal" "$dir"
	FILE_SIZE=1M file-generate "$dir/file"
done

# Remember list of all available chunks, stop one of the chunkservers and wait for replication
chunks_before=$(get_list_of_chunks)
lizardfs_chunkserver_daemon 0 stop
echo "Waiting $replication_timeout for replication..."
end_time=$(date +%s -d "$replication_timeout")
while (( $(date +%s) < end_time )); do
	chunks=$(get_list_of_chunks)
	if [[ "$chunks" == "$chunks_before" ]]; then
		break;
	fi
	sleep 1
done
if [[ "$chunks" != "$chunks_before" ]]; then
	diff=$(diff <(echo "$chunks_before") <(echo "$chunks") | grep '^[<>]' || true)
	test_fail "Replication did not succeed in $replication_timeout. Difference:"$'\n'"$diff"
fi

if [[ $verify_file_content == YES ]]; then
	for ((csid=1; csid < number_of_chunkservers; ++csid)); do
		lizardfs_chunkserver_daemon $csid stop
		file-validate */*
		lizardfs_chunkserver_daemon $csid start
		lizardfs_wait_for_ready_chunkservers $((number_of_chunkservers - 1))
	done
fi
