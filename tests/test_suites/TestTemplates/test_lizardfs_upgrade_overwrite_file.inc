timeout_set 120 minutes

# We create file in some custom goal, e.g. '2', or ec21, or ec32.
# We close $CHUNKSERVERS_DOWN_N chunkservers, so that this file is in undergoal,
# and only minimum number of chunkservers for files to be readable remains on.
# (e.g. we close 2 chunkservers when goal=ec32, or 1 when goal=ec21/2).
# We overwrite the file, then restart and upgrade all LizardFS services.
# New version should then remain, and be replicated to previously closed chunkservers.

# Variables passed from place where this templated is invoked:
# - GOAL (e.g. 2, ec21, ec32)
# - LZFS_MOUNT_COMMAND_AFTER_UPGRADE (mfsmount or mfsmount3, mount command to use after
#                                     upgrade to current LizardFS version was done)

export LZFS_MOUNT_COMMAND="mfsmount"

source test_utils/upgrade.sh

CHUNKSERVERS=3
START_WITH_LEGACY_LIZARDFS=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_EXTRA_CONFIG="CREATE_NEW_CHUNKS_IN_MOOSEFS_FORMAT = $MOOSEFS_CHUNK_FORMAT" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_MIN_TIME = 1|OPERATIONS_DELAY_INIT = 0" \
	setup_local_empty_lizardfs info

REPLICATION_TIMEOUT='12 minutes'

# Start the test with master, two chunkservers and mount running old LizardFS code
# Ensure that we work on legacy version
assert_lizardfsXX_services_count_equals 1 ${CHUNKSERVERS} 1

cd "${info[mount0]}"
mkdir dir
assert_success lizardfsXX mfssetgoal $GOAL dir
cd dir

echo "Starting test"

# map filesize V-> cnt, e.g. 20M -> 5 (5 files of filesize 20M)
declare -A file_count
fill_files_info_maps_with_default_values file_count # always that, or/and add some custom values to arrays here

assert_success generate_files_various_filesizes file_count
echo "All files generated"

# Wait until chunk has been replicated
assert_success wait_for_files_replication file_count "${REPLICATION_TIMEOUT}"
echo "All files replicated (3 parts of each chunk)"

# We stop 1 chunkserver, and overwrite files
lizardfsXX_chunkserver_daemon 0 stop
echo "Chunkserver 0 stopped."

echo "Overwriting half files."
assert_success overwrite_half_files file_count

echo "Validating all files."
assert_success validate_all_files file_count
echo "All files validated."

echo "Listing mounts."
lizardfs_admin_master list-mounts

# Restart all services, start in new version
cd "$TEMP_DIR"
lizardfsXX_chunkserver_daemon 1 stop
lizardfsXX_chunkserver_daemon 2 stop
assert_success lizardfs_mount_unmount 0
lizardfs_admin_master list-mounts
lizardfs_master_daemon restart
lizardfs_mount_start 0
lizardfs_chunkserver_daemon 0 start
lizardfs_chunkserver_daemon 1 start
lizardfs_chunkserver_daemon 2 start

lizardfs_wait_for_ready_chunkservers 3

# Ensure that versions are switched
assert_no_lizardfsXX_services_active

echo "3.13 is On"
# Check if new content was preserved, not old one. On both chunkservers.
cd "${info[mount0]}/dir"
assert_success wait_for_files_replication file_count "${REPLICATION_TIMEOUT}"

echo "ALL REPLICATED"

echo "Validating files with 3/3 CS on"
assert_success validate_all_files file_count

echo "Validating files with CS 1 2 on"
lizardfs_chunkserver_daemon 0 stop
lizardfs_wait_for_ready_chunkservers 2
assert_success validate_all_files file_count

echo "Validating files with CS 0 2 on"
lizardfs_chunkserver_daemon 1 stop
lizardfs_wait_for_ready_chunkservers 1
lizardfs_chunkserver_daemon 0 start
lizardfs_wait_for_ready_chunkservers 2
assert_success validate_all_files file_count

echo "Validating files with CS 0 1 on"
lizardfs_chunkserver_daemon 2 stop
lizardfs_wait_for_ready_chunkservers 1
lizardfs_chunkserver_daemon 1 start
lizardfs_wait_for_ready_chunkservers 2
assert_success validate_all_files file_count

lizardfs_chunkserver_daemon 2 start
lizardfs_wait_for_ready_chunkservers 3
sleep 25m
assert_success validate_all_files file_count

