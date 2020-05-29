parse_si_suffix() {
	if [[ $1 =~ ([0-9]+)(([A-Z]+)?) ]]; then
		local value=${BASH_REMATCH[1]}
		local unit=${BASH_REMATCH[2]}
		local mult=""
		if [[ $unit == "" ]]; then
			echo $value
			return 0
		fi
		case $unit in
			B) mult=1 ;;
			K) mult=1024 ;;
			M) mult=$((1024*1024)) ;;
			G) mult=$((1024*1024*1024)) ;;
			T) mult=$((1024*1024*1024*1024)) ;;
			P) mult=$((1024*1024*1024*1024*1024)) ;;
			E) mult=$((1024*1024*1024*1024*1024*1024)) ;;
			*) return 1 ;;
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

# random_subsets <n> <k> <count> -- echoes 'count' random (n,k)-combinations
random_subsets_of_fixed_size() {
	local n=$1
	local k=$2
	local count=${3:-1}

	python3 ${SOURCE_DIR}/tests/tools/combinations.py --porcelain --random_subsets_of_fixed_size $n $k $count
}

# selected_combinations <n> <k> -- echos selected (n+k, k)-combinations
selected_combinations() {
	local n=$1
	local k=$2

	python3 ${SOURCE_DIR}/tests/tools/combinations.py --porcelain --selected_combinations $n $k
}

# all_subsets_of_fixed_size <n> <k>
all_subsets_of_fixed_size() {
	local n=$1
	local k=$2

	python3 ${SOURCE_DIR}/tests/tools/combinations.py --porcelain --all_combinations $n $k
}
