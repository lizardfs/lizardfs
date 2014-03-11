function get_stack () {
	# bash throws an error "Unbound variable" when dealing with empty arrays
	set +u
	STACK=""
	# to avoid noise we start with $1 + 1 to skip get_stack caller
	# (and other functions, if the user whishes to)
	local skip
	skip=$((${1} + 1))
	local i
	local stack_size=${#FUNCNAME[@]}
	local argv_beg=0 # where arguments for current function begin
	for (( i = 0; i < stack_size; ++i )); do
		local func=${FUNCNAME[$i]}
		local line=${BASH_LINENO[$i]}
		local src=${BASH_SOURCE[$((i + 1))]}
		local arguments_count=${BASH_ARGC[$i]:-0}
		local args=()
		local j
		for (( j = argv_beg + arguments_count - 1; j >= argv_beg; --j )); do
			args+=("'${BASH_ARGV[$j]}'")
		done
		argv_beg=$((argv_beg + arguments_count))
		if (( i >= skip && line > 0 )); then
			# join arguments with commas
			local joined_args=$(IFS=, ; echo "${args[*]}")
			STACK+="$(basename "$src"):$line $func(${joined_args})"$'\n'
		fi
	done
	set -u
}

function print_stack () {
	local skip
	skip=$((${1:-0} + 1))
	get_stack $skip # remove get_stack_echo from stack trace
	# Print red message to the console
	echo -n "$STACK"
	unset STACK
}
