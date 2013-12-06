DEFAULT_TEST_TIMEOUT="30 seconds"

timeout_killer_thread() {
	while ! test_frozen; do
		sleep 1
		if test_frozen; then
			return
		fi
		end_ts=$(cat "$test_timeout_end_ts_file") || true
		now_ts=$(date +%s)
		if [[ -z $end_ts ]]; then
			# A race with timeout_set occured (it truncates the endTS file and then writes it)
			# or a race with test_cleanup (test_timeout_end_ts_file has just been removed)
			continue
		fi
		if (( now_ts >= end_ts )); then
			test_add_failure "Test timed out ($(cat "$test_timeout_value_file"))"
			test_freeze_result
			killall -9 -u $(whoami)
		fi
	done
}

timeout_init() {
	test_timeout_end_ts_file="$TEMP_DIR/$(unique_file)_timeout_endTS.txt"
	test_timeout_value_file="$TEMP_DIR/$(unique_file)_timeout_value.txt"
	timeout_set "$DEFAULT_TEST_TIMEOUT"
	# Parentheses below are needed to make 'wait' command work in tests.
	# They make the killer thread to be a job owned by a temporary subshell, not ours
	( timeout_killer_thread & )
}

timeout_set() {
	echo "$*" > "$test_timeout_value_file"
	date +%s -d "$*" > "$test_timeout_end_ts_file"
}