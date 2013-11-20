parse_si_suffix() {
	if [[ $1 =~ ([0-9]+)((K|M|G|T)?) ]]; then
		local value=${BASH_REMATCH[1]}
		local unit=${BASH_REMATCH[2]}
		local mult=""
		case $unit in
			K) mult=1024 ;;
			M) mult=$((1024*1024)) ;;
			G) mult=$((1024*1024*1024)) ;;
			T) mult=$((1024*1024*1024*1024)) ;;
			*) mult=1 ;;
		esac
		echo $((mult*value))
	else
		return 1
	fi
}

# random <min> <max> -- echoes random number >= min and <= max.
# Examples:
#    random 0 1
#    random 1K 1G
#    random 1 1K
random() {
	[[ $1 && $2 ]] || return 1
	local min=$(parse_si_suffix $1)
	local max=$(parse_si_suffix $2)
	shuf -n 1 --input-range=$min-$max
}

# unique_file [<suffix>] -- generates an unique string
# files created by this function are automatically removed at the end of a test
unique_file() {
	local suffix=""
	if (( $# >= 1 )); then
		suffix="_$1"
	fi
	echo "temp_$(date +%s.%N)_$$$suffix"
}
