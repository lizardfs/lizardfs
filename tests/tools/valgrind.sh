# Not enabled yet.
valgrind_enabled_=

# A script which runs valgrind. Its file name will be generated in valgrind_enable
valgrind_script_=

valgrind_enabled() {
	test -z $valgrind_enabled_ && return 1 || return 0
}

# Enables valgrind, can be called at the beginning of a test case.
valgrind_enable() {
	assert_program_installed valgrind
	valgrind_version=$(valgrind --version | cut -d'-' -f 2)
	if ! version_compare_gte "$valgrind_version" "3.15.0" ; then
		echo " --- Error: Minimum valgrind version supported is 3.15.0 but yours is $valgrind_version --- "
		echo " --- valgrind won't be enabled --- "
		return 1
	fi
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
		valgrind_command+=" --log-file=${ERROR_DIR}/valgrind__\${1}_%p.log"

		# Valgrind errors will generate suppresions:
		valgrind_command+=" --gen-suppressions=all"

		# Valgrind will show filepaths with module subdirectories on errors:
		valgrind_command+=" --fullpath-after=src/"

		# Create a script which will run processes on valgrind. This to make it possible
		# to modify this script to stop spawning new valgrind processes in valgrind_terminate.
		valgrind_script_="$TEMP_DIR/$(unique_file)_valgrind.sh"
		echo -e "#!/usr/bin/env bash\nexec $valgrind_command \"\$@\"" > "$valgrind_script_"
		chmod +x "$valgrind_script_"
		command_prefix="${valgrind_script_} ${command_prefix}"

		echo " --- valgrind enabled in this test case ($(valgrind --version)) --- "
		timeout_set_multiplier 15 # some tests need so big one
	fi
}

# Terminate valgrind processes to get complete memcheck logs from them
valgrind_terminate() {
	# Disable starting new memcheck processes
	local tmpfile="$TEMP_DIR/$(unique_file)_fake_valgrind.txt"
	echo -e "#!/usr/bin/env bash\n\"\$@\"" > "$tmpfile"
	chmod +x "$tmpfile"
	mv "$tmpfile" "$valgrind_script_"
	# Wait a bit if there are any valgrind processes which have just started. This is
	# because of a bug in glibc/valgrind which results in SIGSEGV if we kill memcheck too soon.
	wait_for "! pgrep -u lizardfstest -d, memcheck | xargs -r ps -o etime= -p | grep -q '^ *00:0[0-3]$'" '5 seconds' || true
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
	rm -f /tmp/vgdb-pipe*by-lizardfstest* || true  # clean up any garbage left in /tmp
}
