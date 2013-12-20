#!/bin/bash

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
	sudo pkill -9 -u lizardfstest
	tries=0
	while ps -u lizardfstest | grep -v 'PID.*TTY.*TIME' >/dev/null; do
		if (( tries == 50 )); then
			echo "Cannot stop running tests, still running:" >&2
			ps -u lizardfstest >&2
			exit 1
		fi
		sleep 0.5
		sudo pkill -9 -u lizardfstest
		((tries++))
	done
}

if [[ $# != 1 ]]; then
	echo "Usage: $0 <test_case>" >&2
	exit 1
fi

export ERROR_DIR=/tmp/lizardfs_error_dir
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

nice nice sudo -HEu lizardfstest bash -c "chmod -Rf a+rwX ${ERROR_DIR}"
for log_file_name in `ls "${ERROR_DIR}"` ; do
	log_file="${ERROR_DIR}/${log_file_name}"
	if [[ -s ${log_file} ]]; then
		status=1
		echo "Error in ${log_file_name}" | tee "${ERROR_FILE}"
		cat "${log_file}"
		if [[ $TEST_OUTPUT_DIR ]]; then
			cp "${log_file}" "$TEST_OUTPUT_DIR/$(date '+%F_%T')__$(basename -s .sh $1)__${log_file_name}"
		fi
	fi
done

rm -rf "${ERROR_DIR}"

# Return proper status
exit $status
