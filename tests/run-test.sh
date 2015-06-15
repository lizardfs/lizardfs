#!/usr/bin/env bash

# This file can be used to run tests as a user lizardfstests
# To create such user:
#   sudo adduser --quiet --system --group --home /var/lib/lizardfstest lizardfstest
#   sudo usermod -a -G fuse lizardfstest
# To work as a shell script it also needs the file /etc/sudoers.d/lizardfstest;
# you can create this file with: sudo visudo -f /etc/sudoers.d/lizardfstest
# The file should contain these two lines:
#   ALL ALL = (lizardfstest) NOPASSWD: ALL
#   ALL ALL = NOPASSWD: /usr/bin/pkill -9 -u lizardfstest

stop_tests() {
	local users=$(echo lizardfstest lizardfstest_{0..9})
	local users_list=${users// /,}
	local try_count=0
	# start with killing lizardfstest processes, this will likely suffice
	sudo pkill -9 -u lizardfstest
	sleep 0.1
	while pgrep -u $users_list >/dev/null ; do
		for user in $users; do
			sudo pkill -9 -u $user
		done
		((try_count++))
		if (( try_count == 50 )); then
			echo "Cannot stop running tests, still running:" >&2
			pgrep -u $users_list >&2
			exit 1
		fi
		sleep 0.5
	done
}

if [[ $# != 1 ]]; then
	echo "Usage: $0 <test_case>" >&2
	exit 1
fi
export SOURCE_DIR=$(readlink -m "$(dirname "$0")/..")
export ERROR_DIR=/tmp/lizardfs_error_dir
export LIZARDFS_LOG_ORIGIN=yes # adds file:line:function to debug logs
umask 0022
rm -rf "${ERROR_DIR}"
mkdir "${ERROR_DIR}"
chmod 0777 "${ERROR_DIR}"

# Run the tests
cd "$(dirname "$0")"
stop_tests
test_script="source tools/test_main.sh; test_begin; source '$1'; test_end"
nice nice sudo -HEu lizardfstest bash -c "$test_script"
status=$?
stop_tests

# Remove files left by tests
nice nice sudo -HEu lizardfstest bash -c "source tools/test_main.sh; test_cleanup"
stop_tests # Kill processes left by cleanup

nice nice sudo -HEu lizardfstest sh -c "chmod -Rf a+rwX ${ERROR_DIR}"
for log_file in "$ERROR_DIR"/* ; do
	log_file_name=$(basename "$log_file")
	if [[ -s ${log_file} ]]; then
		status=1
		if [[ $log_file_name != syslog.log ]]; then
			# Do not inform users that there is nonempty syslog
			# It is always nonempty if the test failed
			echo "(FATAL) Errors in ${log_file_name}" | tee "${ERROR_FILE}"
			cat "${log_file}"
		fi
		if [[ $TEST_OUTPUT_DIR ]]; then
			cp "${log_file}" "$TEST_OUTPUT_DIR/$(date '+%F_%T')__$(basename $1 .sh)__${log_file_name}"
		fi
	fi
done

rm -rf "${ERROR_DIR}"

# Return proper status
exit $status
