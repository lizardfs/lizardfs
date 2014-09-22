# Not enabled yet.
valgrind_enabled_=

valgrind_enabled() {
	test -z $valgrind_enabled_ && return 1 || return 0
}

# Enables valgrind, can be called at the beginning of a test case.
valgrind_enable() {
	assert_program_installed valgrind
	if [[ -z $valgrind_enabled_ ]]; then
		valgrind_enabled_=1

		# Build a valgrind invocation which will properly run memcheck.
		local valgrind_command="valgrind -q --tool=memcheck --leak-check=full`
				` --suppressions=$SOURCE_DIR/tests/tools/valgrind.supp"

		# New ( >= 3.9) versions of valgrind support some nice heuristics which remove
		# a couple of false positives (eg. leak reports when there is a reachable std::string).
		# Use the heuristics if available.
		if valgrind --leak-check-heuristics=all true &>/dev/null; then
			valgrind_command+=" --leak-check-heuristics=all"
		fi

		# Valgrind error messages will be written here.
		valgrind_command+=" --log-file=${ERROR_DIR}/valgrind_%p.log"

		# Valgrind errors will generate suppresions:
		valgrind_command+=" --gen-suppressions=all"

		echo " --- valgrind enabled in this test case ($(valgrind --version)) --- "
		command_prefix="${valgrind_command} ${command_prefix}"
		timeout_set_multiplier 10 # some tests need so big one
	fi
}

# Terminate valgrind processes to get complete memcheck logs from them
valgrind_terminate() {
	local pattern='memcheck|polonaise-'
	if pgrep -u lizardfstest "$pattern" >/dev/null; then
		echo " --- valgrind: Waiting for all processes to be terminated --- "
		pkill -TERM -u lizardfstest "$pattern"
		wait_for '! pgrep -u lizardfstest memcheck >/dev/null' '60 seconds' || true
		if pgrep -u lizardfstest "$pattern" >/dev/null; then
			echo " --- valgrind: Stop FAILED --- "
		else
			echo " --- valgrind: Stop OK --- "
		fi
	fi
}
