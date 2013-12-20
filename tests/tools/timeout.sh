timeout_killer_thread() {
	while ! test_frozen; do
		sleep 1
		if test_frozen; then
			return
		fi

		local multiplier=$(cat "$test_timeout_multiplier_file")
		local begin_ts=$(cat "$test_timeout_begin_ts_file") || true
		local value_string=$(cat "$test_timeout_value_file") || true
		local value=$(($(date +%s -d "$value_string") - $(date +%s)))
		local now_ts=$(date +%s)

		if [[ -z $begin_ts || -z $value_string || -z $multiplier ]]; then
			# A race with timeout_set occured (it truncates the endTS file and then writes it)
			# or a race with test_cleanup (test_timeout_end_ts_file has just been removed)
			continue
		fi

		local end_ts=$(($begin_ts + $value * $multiplier))

		if (( now_ts >= end_ts )); then
			if (( multiplier != 1 )); then
				test_add_failure "Test timed out (${multiplier} * ${value_string})"
			else
				test_add_failure "Test timed out (${value_string})"
			fi
			test_freeze_result
			killall -9 -u $(whoami)
		fi
	done
}

timeout_init() {
	test_timeout_begin_ts_file="$TEMP_DIR/$(unique_file)_timeout_beginTS.txt"
	test_timeout_multiplier_file="$TEMP_DIR/$(unique_file)_timeout_multiplier.txt"
	test_timeout_value_file="$TEMP_DIR/$(unique_file)_timeout_value.txt"

	# default timeout values
	timeout_set "30 seconds"
	timeout_set_multiplier 1

	# Parentheses below are needed to make 'wait' command work in tests.
	# They make the killer thread to be a job owned by a temporary subshell, not ours
	( timeout_killer_thread & )
}

timeout_set() {
	echo "$*" > "$test_timeout_value_file"
	date +%s > "$test_timeout_begin_ts_file"
}

timeout_set_multiplier() {
	echo "$1" > "$test_timeout_multiplier_file"
}
