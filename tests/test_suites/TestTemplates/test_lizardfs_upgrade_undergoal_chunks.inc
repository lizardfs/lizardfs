timeout_set 120 minutes

# We create file in some custom goal, but in undergoal (e.g. only 1 chunkserver up,
# when goal is 2, or 3 cs up, when goal is ec32).
# Then we check if it will replicate to other chunkservers
# and be readable after restarting and upgrading all LizardFS services.

# Variables passed from place where this templated is invoked:
# - GOAL (e.g. 2, ec21, ec32)
# - CHUNKSERVERS_GOAL_COVER (compatible with GOAL, e.g. goal 2 -> 2, ec32 -> 5)
# - CHUNKSERVERS_REDUNDANT (how many we turn off, e.g. GOAL ec32 -> 2, ec21 -> 1)
# - MOOSEFS_CHUNK_FORMAT ("1" when old chunk format, "0" otherwise)
CHUNKSERVERS_GOAL_COVER=$(goal_to_part_count ${GOAL})
CHUNKSERVERS_REDUNDANT=$(redundant_parts ${GOAL})
CHUNKSERVERS_MINIMUM=$((CHUNKSERVERS_GOAL_COVER - ${CHUNKSERVERS_REDUNDANT}))
REPLICATION_SPEED="3000000" # Bytes per second

source test_utils/upgrade.sh

export LZFS_MOUNT_COMMAND="mfsmount"

CHUNKSERVERS=$CHUNKSERVERS_GOAL_COVER
	START_WITH_LEGACY_LIZARDFS=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_EXTRA_CONFIG="CREATE_NEW_CHUNKS_IN_MOOSEFS_FORMAT = $MOOSEFS_CHUNK_FORMAT" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_MIN_TIME = 1|OPERATIONS_DELAY_INIT = 0" \
	setup_local_empty_lizardfs info

# Ensure that we work on legacy version, with proper number of services
assert_lizardfsXX_services_count_equals 1 $CHUNKSERVERS_GOAL_COVER 1

# Stop CHUNKSERVER_DOWN_N chunkservers, which will result in files with undergoal
stop_lizardfsXX_chunkservers_from_to $CHUNKSERVERS_MINIMUM $CHUNKSERVERS_GOAL_COVER
assert_lizardfsXX_services_count_equals 1 $CHUNKSERVERS_MINIMUM 1

cd "${info[mount0]}"
mkdir dir
assert_success lizardfsXX mfssetgoal $GOAL dir
cd dir

# map filesize -> cnt, e.g. 20M -> 5 (5 files of filesize 20M)
declare -A file_count
fill_files_info_maps_with_default_values file_count # always that, or/and add some custom values to arrays here

# Generate files and check if they are OK
assert_success generate_files_various_filesizes file_count
check_all_files_readable_and_proper_parts_nr file_count $CHUNKSERVERS_MINIMUM "${REPLICATION_SPEED}"

echo "Files properly created. Stopping legacy services."
cd "$TEMP_DIR"
# Stop all legacy services
stop_lizardfsXX_services 1 $CHUNKSERVERS_MINIMUM 1

# Start recent-version services, all chunkservers now
start_lizardfs_services 1 $CHUNKSERVERS_GOAL_COVER 1

# Ensure that versions are switched
assert_no_lizardfsXX_services_active

cd "${info[mount0]}/dir"
# Check if all files will get replicated to all chunkservers
wait_for_files_replication file_count "${REPLICATION_SPEED}"

# Test if files are readable with all CS on
check_all_files_readable_and_proper_parts_nr file_count $CHUNKSERVERS_GOAL_COVER "${REPLICATION_SPEED}"

# ADD SLEEP HERE
# sleep 25m

# Check if we can still read files with only minimum chunkservers' nr on
# a) Without first n chunkservers [0, CHUNKSERVERS_REDUNDANT)
lizardfs_chunkservers_from_to stop 0 $CHUNKSERVERS_REDUNDANT
check_all_files_readable_and_proper_parts_nr file_count $CHUNKSERVERS_MINIMUM "${REPLICATION_SPEED}"
echo "Files OK, without first x chunkservers."

# b) Without last n chunkservers [x, CHUNKSERVERS_GOAL_COVER)
lizardfs_chunkservers_from_to start 0 $CHUNKSERVERS_REDUNDANT
lizardfs_wait_for_all_ready_chunkservers
lizardfs_chunkservers_from_to stop $CHUNKSERVERS_MINIMUM $CHUNKSERVERS_GOAL_COVER
check_all_files_readable_and_proper_parts_nr file_count $CHUNKSERVERS_MINIMUM "${REPLICATION_SPEED}"
echo "Files OK, without last x chunkservers"
