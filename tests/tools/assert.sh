# Assertion types:
# * expect_some_condition -- adds error to the test results, but continues the test
# * assert_some_condition -- adds error to the test results and immediately stops the test
# * assertlocal_some_condition -- adds error to the results and exits current subshell

# (assert|assertlocal|expect)_program_installed <program>...
assert_template_program_installed_() {
	for program in "$@"; do
		if ! is_program_installed "$program"; then
			$FAIL_FUNCTION "$program is not installed"
		fi
	done
}

# (assert|assertlocal|expect)_file_exists <file>
assert_template_file_exists_() {
	if [[ ! -e "$1" ]]; then
		$FAIL_FUNCTION "File '$1' does not exist"
	fi
}

# (assert|assertlocal|expect)_file_not_exists <file>
assert_template_file_not_exists_() {
	if [[ -e "$1" ]]; then
		$FAIL_FUNCTION "File '$1' does exist"
	fi
}

# (assert|assertlocal|expect)_files_equal <file1> <file2>
assert_template_files_equal_() {
	local file1=$1
	local file2=$2
	if ! cmp "$file1" "$file2"; then
		$FAIL_FUNCTION "Files $file1 and $file2 are different"
	fi
}

# (assert|assertlocal|expect)_less_or_equal <number1> <number2>
assert_template_less_or_equal_() {
	if (( $1 > $2 )); then
		$FAIL_FUNCTION "Expected: $1 <= $2"
	fi
}

# (assert|assertlocal|expect)_less_than <number1> <number2>
assert_template_less_than_() {
	if (( $1 >= $2 )); then
		$FAIL_FUNCTION "Expected: $1 < $2"
	fi
}

# (assert|assertlocal|expect)_not_equal <string1> <string2>
assert_template_not_equal_() {
	if [[ "$1" == "$2" ]]; then
		$FAIL_FUNCTION "Expected string different than $1"
	fi
}

# (assert|assertlocal|expect)_equals <expected_string> <actual_string>
assert_template_equals_() {
	if [[ "$1" != "$2" ]]; then
		$FAIL_FUNCTION "Expected: $1, got: $2"
	fi
}

# (assert|assertlocal|expect)_matches <regex> <string>
assert_template_matches_() {
	if [[ ! "$2" =~ $1 ]]; then
		$FAIL_FUNCTION "Expected: '$2' to match regex '$1'"
	fi
}

# (assert|assertlocal|expect)_near <expected_number> <actual_number> <max_absolute_error>
assert_template_near_() {
	if ! (( $1 - $3 <= $2 && $2 <= $1 + $3 )); then
		$FAIL_FUNCTION "Expected: $1 +/- $3, got: $2"
	fi
}

# (assert|assertlocal|expect)_success <command> [<args>...]
assert_template_success_() {
	local error_msg  # a local variable for errors printed by <command>
	local fd         # a local variable to hold a file descriptor
	exec {fd}>&1     # duplicate stdout to a descriptor and store its number in the 'fd' variable
	# Now run "$@" (the command under a test) in the following way:
	# - its stderr goes to the 'error_msg' variable
	# - its stdout goes to the current stdout (duplicated to $fd to make it available in a subshell)
	if ! error_msg=$("$@" 2>&1 1>&$fd); then
		$FAIL_FUNCTION "Command '$*' failed. Standard error:"$'\n'"$error_msg"
	fi
	exec {fd}>&-     # close the descriptor stored in the 'fd' variable
}

# (assert|assertlocal|expect)_failure <command> [<args>...]
assert_template_failure_() {
	if "$@"; then
		$FAIL_FUNCTION "Command '$*' succeeded"
	fi
}

# (assert|assertlocal|expect)_awk_finds <awk-condition> <string>
assert_template_awk_finds_() {
	local condition=$1
	local string=$2
	local matches=$(awk "$condition" <<< "$string")
	local lines=$(awk "$condition" <<< "$string" | wc -l)
	if (( lines == 0 )); then
		$FAIL_FUNCTION "Expected line matching '$condition' to be found in:"$'\n'"$string"
	fi
}

# (assert|assertlocal|expect)_awk_finds_no <awk-condition> <string>
assert_template_awk_finds_no_() {
	local condition=$1
	local string=$2
	local matches=$(awk "$condition" <<< "$string")
	local lines=$(awk "$condition" <<< "$string" | wc -l)
	if (( lines > 0 )); then
		local msg_header="Expected line matching '$condition' not to be found in:"$'\n'"$string"
		local msg_footer="But the following has been found:"$'\n'"$matches"
		$FAIL_FUNCTION "$msg_header"$'\n'"$msg_footer"
	fi
}

# (assert|assertlocal|expect)_no_diff <string1> <string2>
assert_template_no_diff_() {
	local diff=$(diff -u5 <(echo -n "$1") <(echo -n "$2")) || true
	if [[ -n "$diff" ]]; then
		$FAIL_FUNCTION $'Strings are different:\n'"$diff"
	fi
}

# (assert|assertlocal|expect)_eventually <command> [<timeout>]
assert_template_eventually_() {
	local command=$1
	local timeout=$(rescale_timeout_for_assert_eventually_ "${2:-}")
	if ! wait_for "$command" "$timeout"; then
		$FAIL_FUNCTION "'$command' didn't succedd within $timeout"
	fi
}

# (assert|assertlocal|expect)_empty <string>
assert_template_empty_() {
	if [[ -n $1 ]]; then
		if [[ $(wc -l <<< "$1") == 1 ]]; then
			$FAIL_FUNCTION "Expected empty string, got '$1'"
		else
			$FAIL_FUNCTION $'Expected empty string, got:\n'"$1"
		fi
	fi
}

# (assert|assertlocal|expect)_eventually_prints <string> <command> [<timeout>]
assert_template_eventually_prints_() {
	local string=$1
	local command=$2
	local timeout=$(rescale_timeout_for_assert_eventually_ "${3:-}")
	if ! wait_for "[[ \$($command) == \"$string\" ]]" "$timeout"; then
		$FAIL_FUNCTION "'$command' didn't print '$string' within $timeout. "`
				`"It prints now: '$(eval "$command" || true)'"
	fi
}

# (assert|assertlocal|expect)_eventually_matches <regex> <command> [<timeout>]
assert_template_eventually_matches_() {
	local regex="$1"
	local command="$2"
	local timeout=$(rescale_timeout_for_assert_eventually_ "${3:-}")
	if ! wait_for "[[ \$($command) =~ \$regex ]]" "$timeout"; then
		$FAIL_FUNCTION "'$command' didn't print output matching '$regex' within $timeout. "`
				`"It prints now: '$(eval "$command" || true)'"
	fi
}

# (assert|assertlocal|expect)_eventually_equals <command1> <command2> [<timeout>]
assert_template_eventually_equals_() {
	local command1=$1
	local command2=$2
	local timeout=$(rescale_timeout_for_assert_eventually_ "${3:-}")
	if ! wait_for "[[ \$($command1) == \$($command2) ]]" "$timeout"; then
		diff="$(diff -u5 <(eval "$command1") <(eval "$command2") || true)"
		$FAIL_FUNCTION "'$command1' didn't output the same as '$command2' within $timeout`
				`"$'\n'"$diff"
	fi
}

# This function returns a line from some source file of this test suite
test_absolute_path_=$(readlink -m .)
get_source_line() {
	local file=$1
	local line=$2
	( cd "$test_absolute_path_" ; sed -n "${line}s/^[[:blank:]]*//p" "$file" || true)
}

# Internal functions
rescale_timeout_for_assert_eventually_() {
	if valgrind_enabled; then
		if [[ -n "$1" ]]; then
			local multiplier=$(timeout_get_multiplier)
			local value=$(($(date +%s -d "$1") - $(date +%s)))
			echo $((value * multiplier)) seconds
		else
			echo 60 seconds
		fi
	else
		if [[ -n "$1" ]]; then
			echo "$1"
		else
			echo 15 seconds
		fi
	fi
}

create_error_message_() {
	local message=${MESSAGE:-}
	local call=$(get_source_line "$ASSERT_FILE" "$ASSERT_LINE")
	local assertion=$(grep -o "$ASSERT_NAME.*" <<< "$call" || true)
	if [[ $message ]]; then
		echo -n "$message: "
	fi
	if [[ $assertion ]]; then
		echo "Assertion '$assertion' failed"
	else
		echo "Assertion failed"
	fi
	echo "$*"
	echo "Location: $(basename "$ASSERT_FILE"):$ASSERT_LINE"
	echo "Backtrace:"
	# remove top 3 function calls from stack trace: create_error_message_, do_*_failed_, assert_template_*
	print_stack 3
}

do_assert_failed_() {
	test_fail "$(create_error_message_ "$*")"
}

do_assertlocal_failed_() {
	test_add_failure "$(create_error_message_ "$*")"
	exit 1
}

do_expect_failed_() {
	test_add_failure "$(create_error_message_ "$*")"
}

# Create expect/assert functions for all templates defined above
for template in $(typeset -F | grep -o 'assert_template_.*_'); do
	for type in assert assertlocal expect; do
		function_name=$(echo $template | sed -re "s/assert_template_(.*)_/${type}_\1"/)
		context="ASSERT_NAME=$function_name ASSERT_FILE=\"\${BASH_SOURCE[1]}\" ASSERT_LINE=\${BASH_LINENO[0]}"
		body="export $context ; FAIL_FUNCTION=do_${type}_failed_ $template \"\$@\""
		eval "$function_name() { $body ; }"
	done
done
unset function_name context body
