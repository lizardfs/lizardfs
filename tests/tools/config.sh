# Load config file with machine-specific configuration
if [[ ! -z "${LIZARDFS_TESTS_CONF:-}" && -f "${LIZARDFS_TESTS_CONF}" ]]; then
	echo "Using \"${LIZARDFS_TESTS_CONF}\" tests configuration file"
	. "${LIZARDFS_TESTS_CONF}"
elif [[ -f /home/${SUDO_USER}/etc/lizardfs/tests.conf ]]; then
	echo "Using \"/home/${SUDO_USER}/etc/lizardfs/tests.conf\" tests configuration file"
	. /home/${SUDO_USER}/etc/lizardfs/tests.conf
elif [[ -f /etc/lizardfs_tests.conf ]]; then
	echo "Using the default \"/etc/lizardfs_tests.conf\" tests configuration file"
	. /etc/lizardfs_tests.conf
fi

# Set up the default configuration values if not set yet
# This is a list of all configuration variables, that these tests use
: ${LIZARDFS_DISKS:=}
: ${LIZARDFS_LOOP_DISKS:=}
: ${TEMP_DIR:=/tmp/LizardFS-autotests}
: ${MOOSEFS_DIR:=/tmp/LizardFS-autotests-mfs}
: ${LIZARDFSXX_DIR_BASE:=/tmp/LizardFS-autotests-old}
: ${LIZARDFS_ROOT:=$HOME/local}
: ${FIRST_PORT_TO_USE:=9600}
: ${ERROR_FILE:=}
: ${RAMDISK_DIR:=/mnt/ramdisk}
: ${TEST_OUTPUT_DIR:=$TEMP_DIR}
: ${USE_VALGRIND:=}
: ${DEBUG:=}
: ${DEBUG_LEVEL:=0}

# This has to be an absolute path!
TEMP_DIR=$(readlink -m "$TEMP_DIR")
mkdir -p "$TEMP_DIR"
chmod 777 "$TEMP_DIR"

# Prepare important environment variables
export PATH="$LIZARDFS_ROOT/sbin:$LIZARDFS_ROOT/bin:$PATH"

# Quick checks needed to call test_begin and test_fail
if (( BASH_VERSINFO[0] * 100 + BASH_VERSINFO[1] < 402 )); then
	echo  "Error: bash v4.2 or newer required, but $BASH_VERSION found" >&2
	exit 1
fi
if ! touch "$TEMP_DIR/check_tmp_dir" || ! rm "$TEMP_DIR/check_tmp_dir"; then
	echo "Configuration error: cannot create files in $TEMP_DIR" >&2
	exit 1
fi

# This function shold be called just after test_fail is able to work
check_configuration() {
	for prog in \
			$LIZARDFS_ROOT/sbin/{mfsmaster,mfschunkserver} \
			$LIZARDFS_ROOT/bin/lizardfs \
			$LIZARDFS_ROOT/bin/file-generate \
			$LIZARDFS_ROOT/bin/file-validate
	do
		if ! [[ -x $prog ]]; then
			test_fail "Configuration error, executable $prog not found"
		fi
	done

	if [[ ! -x $LIZARDFS_ROOT/bin/mfsmount ]] && [[ ! -x $LIZARDFS_ROOT/bin/mfsmount3 ]]; then
		test_fail "Configuration error, mfsmount executable ($LIZARDFS_ROOT/bin/mfsmount or $LIZARDFS_ROOT/bin/mfsmount3) not found"
	fi

	if ! df -T "$RAMDISK_DIR" | grep "tmpfs\|ramfs" >/dev/null; then
		test_fail "Configuration error, ramdisk ($RAMDISK_DIR) is missing"
	fi

	for dir in "$TEMP_DIR" "$RAMDISK_DIR" "$TEST_OUTPUT_DIR" $LIZARDFS_LOOP_DISKS; do
		if [[ ! -w $dir ]]; then
			test_fail "Configuration error, cannot create files in $dir"
		fi
	done

	if ! cat /etc/fuse.conf >/dev/null; then
		test_fail "Configuration error, user $(whoami) is not a member of the fuse group"
	fi

	if ! grep '[[:blank:]]*user_allow_other' /etc/fuse.conf >/dev/null; then
		test_fail "Configuration error, user_allow_other not enabled in /etc/fuse.conf"
	fi
}
