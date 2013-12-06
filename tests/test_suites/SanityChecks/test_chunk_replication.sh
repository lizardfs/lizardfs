timeout_set 90 seconds

CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1|REPLICATIONS_DELAY_INIT = 0" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

# Create file consisting of 3 chunks with goal=1 and check if it's true
dd if=/dev/zero of=file bs=1M count=130
fileinfo=$(mfscheckfile file)
if ! echo "$fileinfo" | grep 'chunks with 1 copy: *3$' >/dev/null; then
	test_fail "Expected 3 chunks with 1 copy, got:"$'\n'"$fileinfo"
fi

# Increase the goal
echo "Increasing goal"
mfssetgoal 3 file

# We will wait for 60 seconds, no longer!
timeout=60
end_time=$((timeout + $(date +%s)))
is_goal_ok=
while (( $(date +%s) < end_time )); do
	if mfscheckfile file | grep 'chunks with 3 copies: *3$' >/dev/null; then
		is_goal_ok=true
		break
	fi
	sleep 1
done
if ! [[ $is_goal_ok ]]; then
	test_add_failure "Number of copies did not increase within $timeout seconds"
fi
