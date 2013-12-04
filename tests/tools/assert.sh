# assert_program_installed <program>
assert_program_installed() {
	local program=$1
	if ! is_program_installed "$program"; then
		assert_failed_ "$program is not installed"
	fi
}

# expect_files_equal <file1> <file2>
expect_files_equal() {
	local file1=$1
	local file2=$2
	if ! cmp "$file1" "$file2"; then
		expect_failed_ "Files $file1 and $file2 are different"
	fi
}

# expect_success <command> [<args>...]
expect_success() {
	if ! "$@"; then
		expect_failed_ "Command '$*' failed"
	fi
}

# Internal functions

assert_failed_() {
	local message=${MESSAGE:-}
	if [[ $message ]]; then
		test_fail "$message: $*"
	else
		test_fail "$*"
	fi
}

expect_failed_() {
	local message=${MESSAGE:-}
	if [[ $message ]]; then
		test_add_failure "$message: $*"
	else
		test_add_failure "$*"
	fi
}
