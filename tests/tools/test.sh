# Call this in a test case to mark test as failed, but continue running
test_add_failure() {
	if test_frozen; then
		return
	fi
	local message="[$(date +"%F %T")] $*"
	# Env. valiable ERROR_FILE can be used to store error messages in a file
	echo "$message" | tee -a "${ERROR_FILE:-/dev/null}" >> "$test_result_file"
	# Print bold red message to the console
	tput setaf 1
	tput bold
	echo "FAILURE $message"
	tput sgr0
}

# Call this in a test case to mark test as failed and stop it
test_fail() {
	if test_frozen; then
		return
	fi
	test_add_failure "(FATAL) $*"
	test_end
}

# Call this in a test case to make the result of a test not
# change when calling test_fail or test_add_failure
# Useful before cleaning the test using commands like killall
test_freeze_result() {
	touch "$test_end_file"
}

# This checks if the test if frozen
test_frozen() {
	[[ -f "$test_end_file" ]]
}

# You can call this function in a test case to immediatelly end the test.
# You don't have to; it will be called automatically at the end of the test.
test_end() {
	test_freeze_result
	local errors=$(cat "$test_result_file")
	# Disable error checking (we want to be able to return non-zero status) and end the test
	trap - ERR
	set +eE
	[[ -z $errors ]] # This sets the exit status to non-zero if there are errors
	exit
}

# Do not run directly in test cases
# This should be called at the very beginning of a test
test_begin() {
	test_result_file=$TEMP_DIR/$(unique_file)_results.txt
	test_end_file=$test_result_file.end
	check_configuration
	test_cleanup
	touch "$test_result_file"
	trap 'trap - ERR; set +eEu; catch_error_ "$BASH_SOURCE" "$LINENO" "$FUNCNAME"; exit 1' ERR
	set -E
	timeout_init
}

# Do not use directly
# This removes all temporary files and unmounts filesystems
test_cleanup() {
	# Unmount all mfsmounts
	retries=0
	pkill -9 mfsmount || true
	while list_of_mounts=$(cat /etc/mtab | grep mfs | grep fuse); do
		echo "$list_of_mounts" | awk '{print $2}' | \
				xargs -r -d'\n' -n1 fusermount -u || sleep 1
		if ((++retries == 30)); then
			echo "Can't unmount: $list_of_mounts" >&2
			break
		fi
	done
	# Remove temporary files
	if ! [[ $TEMP_DIR ]]; then
		echo "TEMP_DIR variable empty, cowardly refusing to rm -rf /*"
		exit 1
	fi
	rm -rf "$TEMP_DIR"/*
	# Clean ramdisk
	if [[ $RAMDISK_DIR ]]; then
		rm -rf "$RAMDISK_DIR"/*
	fi
	# Clean the disks used by chunkservers
	for d in $LIZARDFS_DISKS; do
		rm -rf "$d"/[0-9A-F][0-9A-F]
	done
}

catch_error_() {
	local file=$1
	local line=$2
	local funcname=$3
	local line_contents=$(sed -n "${line}s/^[[:blank:]]*//p" "$file")
	if [[ $funcname ]]; then
		local location="in function $funcname ($file:$line)"
	else
		local location="($file:$line)"
	fi
	test_add_failure "Command '$line_contents' failed $location"
}
