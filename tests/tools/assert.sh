# Assertion types:
# * expect_some_condition -- adds error to the test results, but continues the test
# * assert_some_condition -- adds error to the test results and immediately stops the test
# * assertlocal_some_condition -- adds error to the results and exits current subshell 
 
# (assert|assertlocal|expect)_program_installed <program>
assert_template_program_installed_() {
	local program=$1
	if ! is_program_installed "$program"; then
		$FAIL_FUNCTION "$program is not installed"
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
		$FAIL_FUNCTION "Expected: '$1' <= '$2'"
	fi
}

# (assert|assertlocal|expect)_equals <string1> <string2>
assert_template_equals_() {
	if [[ $1 != $2 ]]; then
		$FAIL_FUNCTION "Expected: '$1', got:'$2'"
	fi
}

# (assert|assertlocal|expect)_success <command> [<args>...]
assert_template_success_() {
	if ! "$@"; then
		$FAIL_FUNCTION "Command '$*' failed"
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
