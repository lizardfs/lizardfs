# Not enabled yet.
valgrind_enabled=

# Enables valgrind, can be called at the beginning of a test case.
enable_valgrind () {
	if ! is_program_installed valgrind; then
		test_fail "valgrind not installed"
	fi
	if [[ -z $valgrind_enabled ]]; then
		valgrind_enabled=1

		# Valgrind error messages will be written here.
		valgrind_log="${ERROR_DIR}/valgrind_%p.log"
		valgrind_command="valgrind -q --leak-check=full --log-file=${valgrind_log}"

		echo --- valgrind enabled in this test case ---
		command_prefix="${valgrind_command} ${command_prefix}"
		timeout_set_multiplier 5  # some tests need so big one
	fi
}
