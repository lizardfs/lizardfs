wait_for() {
	local goal=$1
	local time_limit="$2"
	local end_ts=$(date +%s -d "$time_limit")
	while (( $(date +%s) < end_ts )); do
		if eval "$goal"; then
			return 0
		fi
		sleep 0.3
	done
	eval "$goal"
}
