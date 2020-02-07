test_function_exists() {
	declare -f -F $1 > /dev/null
	return $?
}

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

# You can call this function in a test case to immediately end the test.
# You don't have to; it will be called automatically at the end of the test.
test_end() {
	trap - DEBUG
	if [[ ${DEBUG} ]]; then
		set +x
	fi
	test_freeze_result
	# some tests may leave pwd at mfs mount point, causing a lockup when we stop mfs
	cd
	if valgrind_enabled; then
		valgrind_terminate
	fi
	# terminate all LizardFS daemons if requested (eg. to collect some code coverage data)
	if [[ ${GENTLY_KILL:-} ]]; then
		for i in {1..50}; do
			local pattern='mfs|lizardfs-polo|polonaise-'
			pkill -USR1 -u lizardfstest "$pattern" || true
			if ! pgrep -u lizardfstest "$pattern" >/dev/null; then
				echo "All LizardFS processes terminated"
				break
			fi
			sleep 0.2
		done
	fi
	local errors=$(cat "$test_result_file")
	if [[ $errors ]]; then
		exit 1
	else
		# Remove log file from ERROR_DIR, because it would cause the test to fail
		if is_centos_system; then
			rm --force "$ERROR_DIR/messages"
		else
			rm --force "$ERROR_DIR/syslog.log"
		fi
		exit 0
	fi
}

debug_command() {
	local depth=${#BASH_SOURCE[@]}
	if (( --depth < 1 )); then
		return 0;
	fi
	if (( depth <= DEBUG_LEVEL )); then
		local indent=">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
		echo "${indent:0:$depth} $(basename "${BASH_SOURCE[1]}"):${BASH_LINENO[0]}:`
				`${FUNCNAME[1]}(): ${BASH_COMMAND}" >&2 | true
	fi
}

# Do not run directly in test cases
# This should be called at the very beginning of a test
test_begin() {
	if is_centos_system; then
			(tail --lines=0 --follow /var/log/messages | stdbuf --output=L tee "$ERROR_DIR/messages" &)
	else
			(tail --lines=0 --follow /var/log/syslog | stdbuf --output=L tee "$ERROR_DIR/syslog.log" &)
	fi
	test_result_file="$TEMP_DIR/$(unique_file)_results.txt"
	test_end_file=$test_result_file.end
	check_configuration
	test_cleanup
	touch "$test_result_file"
	trap 'trap - ERR; set +eE; catch_error_ "${BASH_SOURCE:-}" "${LINENO:-}" "${FUNCNAME:-}"; exit 1' ERR
	set -E
	timeout_init
	system_init
	if [[ ${USE_VALGRIND} ]]; then
		valgrind_enable
	fi
	if [[ ${DEBUG} ]]; then
		export PS4='+$(basename "${BASH_SOURCE:-}"):${LINENO:-}:${FUNCNAME[0]:+${FUNCNAME[0]}():} '
		set -x
	fi
	if (( $DEBUG_LEVEL >= 1 )); then
		trap 'debug_command' DEBUG
		set -T
	fi
}

# Try to chmod 777 all files and subdirectories in given directory using passwordless sudo
mass_chmod_777() {
	find "$1" -mindepth 1 -exec bash -c \
		'user=$(stat -c "%U" "{}"); sudo -nu $user setfacl -b "{}" ; sudo -nu $user chmod 777 "{}"' \;
}

# Do not use directly
# This removes all temporary files and unmounts filesystems
test_cleanup() {
	# Unmount all mfsmounts
	local retries=0
	pkill -KILL -u lizardfstest mfsmount || true
	pkill -KILL -u lizardfstest memcheck || true
	# Search for all fuse filesystems mounted by user lizardfstest and umount them
	local uid=$(id -u lizardfstest)
	while list_of_mounts=$(cat /proc/mounts | awk '$3 ~ /^fuse/' | grep "user_id=$uid[^0-9]"); do
		echo "$list_of_mounts" | awk '{print $2}' | xargs -r -d'\n' -n1 fusermount -u || sleep 1
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
	if ! rm -rf "$TEMP_DIR"/* 2>/dev/null; then
		# try to remove other users' junk the slow way
		mass_chmod_777 "$TEMP_DIR"
		rm -rf "$TEMP_DIR"/*
	fi
	# Clean ramdisk
	if [[ $RAMDISK_DIR ]]; then
		if ! rm -rf "$RAMDISK_DIR"/* 2>/dev/null; then
			mass_chmod_777 "$RAMDISK_DIR"
			rm -rf "$RAMDISK_DIR"/*
		fi
	fi
	# Clean the disks used by chunkservers
	for d in $LIZARDFS_DISKS $LIZARDFS_LOOP_DISKS; do
		rm -rf "$d"/chunks[0-9A-F][0-9A-F]
		rm -f "$d"/.lock
	done
}

catch_error_() {
	# Call test cleanup function
	if test_function_exists test_error_cleanup ; then
		test_error_cleanup || true
	fi

	local file=$1
	local line=$2
	local funcname=$3
	if [[ $funcname ]]; then
		local location="in function $funcname ($(basename "$file"):$line)"
	else
		local location="($file:$line)"
	fi
	# print_stack 1 removes catch_error_ from stack trace
	local stack=$(print_stack 1)
	local command=$(get_source_line "$file" "$line")
	test_add_failure "Command '$command' failed $location"$'\nBacktrace:\n'"$stack"
}
