# Not enabled yet.
valgrind_enabled_=

# Enables valgrind, can be called at the beginning of a test case.
enable_valgrind() {
	assert_program_installed valgrind
	if [[ -z $valgrind_enabled_ ]]; then
		valgrind_enabled_=1

		# Build a valgrind invocation which will properly run memcheck.
		local valgrind_command="valgrind -q --tool=memcheck --leak-check=full"

		# New ( >= 3.9) versions of valgrind support some nice heuristics which remove
		# a couple of false positives (eg. leak reports when there is a reachable std::string).
		# Use the heuristics if available.
		if valgrind --leak-check-heuristics=all true &>/dev/null; then
			valgrind_command+=" --leak-check-heuristics=all"
		fi

		# Valgrind error messages will be written here.
		valgrind_command+=" --log-file=${ERROR_DIR}/valgrind_%p.log"

		echo " --- valgrind enabled in this test case ($(valgrind --version)) --- "
		command_prefix="${valgrind_command} ${command_prefix}"
		timeout_set_multiplier 10 # some tests need so big one
	fi
}
