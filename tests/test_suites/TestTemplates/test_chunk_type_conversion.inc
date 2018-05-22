CHUNKSERVERS=$NUMBER_OF_CHUNKSERVERS \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1|CHUNKS_LOOP_MAX_CPU = 90|OPERATIONS_DELAY_INIT = 0|`
	`ACCEPTABLE_DIFFERENCE = 10.0|REDUNDANCY_LEVEL = 10|CHUNKS_REBALANCING_BETWEEN_LABELS = 1" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

# echo "OK" on success, echo <some error message> otherwise
verify_file_goal() {
	file=$1;
	goal=$(lizardfs getgoal "$file" | awk '{print $NF}')

	if [[ $goal =~ xor([2-9]|10) ]]; then
		level=${BASH_REMATCH[1]}
		# Check if all parts are present:
		for i in $(seq $((level+1))); do
			lizardfs fileinfo "$file" | grep "part $i/$((level+1)) of $goal$" > /dev/null \
					|| { echo "$file: missing part $i of $goal"; return; };
		done
	elif [[ $goal =~ ([1-9]) ]]; then
		level=${BASH_REMATCH[1]}

		copies=$(lizardfs fileinfo "$file" | egrep 'copy.*:[a-zA-Z0-9_]+$' | sort | uniq | wc -l)
		if (( $level > $copies )); then
			echo "$file: expected $level copies, have $copies"
			lizardfs fileinfo "$file"
			return
		fi
	else
		test_fail "Unknown 'lizardfs getgoal $file' output: $goal"
	fi

	echo -n "OK"
}

# In this test we create file with a given goal, change this goal to some other value and then
# check if the file can be read from the just created chunks. We try to test reading from all
# created chunks (and chunk parts) by reading the data with one chunkserver stop, for all
# combinations of files and chunkservers.

# We'll create a file with a goal from the list below and change this file goal to the next value
# from the list. By doing this we will test almost all possible changes of the goal value.
goals="$GOALS_TO_BE_TESTED"

# Create files with goals from the $goals list
for goal in $goals; do
	mkdir "dir_$goal"
	lizardfs setgoal "$goal" "dir_$goal"
	FILE_SIZE=1M file-generate "dir_$goal/file"
done
old_chunks=$(find_all_chunks)

# Change the files goal to the next value from the list
prev_goal=$(echo $goals | tr " " "\n" | tail -n1) # last one on the $goals list
for next_goal in $goals; do
	file="dir_${prev_goal}/file"
	lizardfs setgoal "$next_goal" "$file"
	prev_goal="$next_goal"
done

# Verify if all chunks that should be created were indeed created
# We will wait for $WAIT_FOR_REPLICATION seconds, no longer!
timeout=$(timeout_rescale_seconds "$WAIT_FOR_REPLICATION")
end_time=$((timeout + $(date +%s)))
replication_status=
while (( $(date +%s) < end_time )); do
	for goal in $goals; do
		replication_status=$(verify_file_goal "dir_$goal/file")
		if [[ $replication_status != OK ]]; then
			break; # One of the files didn't yet (hopefully) replicate
		fi
	done
	if [[ $replication_status == OK ]]; then
		break; # Replication finished, going aboard
	fi
	sleep 1
done
if [[ $replication_status != OK ]]; then
	test_fail "Replication did not succeed in $timeout seconds; $replication_status"
fi
if [[ $VERIFY_FILE_CONTENT == NO ]]; then
	test_end
fi

# After verifying that all chunks that should be created were created, remove all the old copies
# and restart chunkservers to make them notice this removal
for csid in $(seq 0 $(($NUMBER_OF_CHUNKSERVERS - 1))); do
	lizardfs_chunkserver_daemon $csid stop &
done
wait
echo "$old_chunks" | xargs -d'\n' rm -f || true  # Some of the files might already be removed
for csid in $(seq 0 $(($NUMBER_OF_CHUNKSERVERS - 1))); do
	lizardfs_chunkserver_daemon $csid start &
done
wait

# Check if all files are readable from the new copies with one of the chunkservers turned off
# (so hopefully we'll check if all the files that were created are valid)
for csid in $(seq 0 $(($NUMBER_OF_CHUNKSERVERS - 1))); do
	lizardfs_wait_for_all_ready_chunkservers
	lizardfs_chunkserver_daemon $csid stop
	for goal in $goals; do
		file="dir_$goal/file"
		file-validate "$file"
	done
	lizardfs_chunkserver_daemon $csid start
done
