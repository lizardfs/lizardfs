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
	local min=$(parse_si_suffix $1)
	local max=$(parse_si_suffix $2)
	shuf -n 1 --input-range=$min-$max
}

# random_log <base> <power> -- echoes random number with log distribution from 0 up to <base>^(power)-1
# warning: this function doesn't check if the number limit (64-bit int?) is exceeded
random_log() {
	if ((($1 < 1) || ($2 < 0))); then
		return 1
	fi

	local base=$1
	local max_figure=$((base - 1))
	local max_power=$2
	local power=$(shuf -n 1 -i 0-$((max_power - 1)))
	local result=0
	while ((power >= 0)); do
		figure=$(shuf -n 1 -i 0-$max_figure)
		result=$(echo "$result + $figure * $base ^ $power" | bc)
		power=$((--power))
	done
	echo $result
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

# pseudorandom_init [<seed>] -- sets a seed (default or specified) for pseudorandom generator
pseudorandom_init() {
	pseudorandom_seed_file_="$TEMP_DIR/$(unique_file pseudorandom_seed)"

	if (( $# >= 1 )); then
		local seed=$1
	else
		local seed=115249 # some prime number
	fi
	echo $seed > "$pseudorandom_seed_file_"
}

# pseudo random number generator with glibc algorithm (drawing bits 0..30)
prng() {
	if [[ ! -e "$pseudorandom_seed_file_" ]]; then
		return 1
	fi

	local seed=$(cat "$pseudorandom_seed_file_")
	seed=$(( (1103515245 * seed + 12345) % 2147483648 ))
	echo $seed | tee "$pseudorandom_seed_file_"
}

# pseudorandom <min> <max> -- echoes random number >= min and <= max up to 2^62
# Examples:
#    pseudorandom 0 1
#    pseudorandom 1K 1G
#    pseudorandom 1 1K
pseudorandom() {
	local min=$(parse_si_suffix $1)
	local max=$(parse_si_suffix $2)
	local shift=2147483648 # 2^31
	local pass1=$(prng)
	local pass2=$(( $shift * $(prng) ))
	local result=$(( min + (pass1 + pass2) % (max - min + 1) ))
	echo $result
}
