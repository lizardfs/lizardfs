timestamp() {
	date +%s
}

nanostamp() {
	date +%s%N
}

wait_for() {
	local goal="$1"
	local time_limit="$2"
	local end_ts="$(date +%s%N -d "$time_limit")"
	while (( $(date +%s%N) < end_ts )); do
		if eval "${goal}"; then
			return 0
		fi
		sleep 0.1
	done
	if eval "${goal}"; then
		return 0
	fi
	return 1
}
