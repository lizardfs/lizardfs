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

# (assert|assertlocal|expect)_success <command> [<args>...]
assert_template_success_() {
	if ! "$@"; then
		$FAIL_FUNCTION "Command '$*' failed"
	fi
}

# Internal functions

add_custom_error_message_() {
	local message=${MESSAGE:-}
	if [[ $message ]]; then
		echo "$message: $*"
	else
		echo "$*"
	fi
}

do_assert_failed_() {
	test_fail $(add_custom_error_message_ "$*")
}

do_assertlocal_failed_() {
	test_add_failure $(add_custom_error_message_ "$*")
	exit 1
}

do_expect_failed_() {
	test_add_failure $(add_custom_error_message_ "$*")
}

# Create expect/assert functions for all templates defined above
for template in $(typeset -F | grep -o 'assert_template_.*_'); do
	for type in assert assertlocal expect; do
		function_name=$(echo $template | sed -re "s/assert_template_(.*)_/${type}_\1"/)
		eval "$function_name() { FAIL_FUNCTION=do_${type}_failed_ $template \"\$@\" ; }"
	done
done
unset function_name
