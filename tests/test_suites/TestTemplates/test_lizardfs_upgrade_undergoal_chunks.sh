timeout_set 1000 minutes

# We create file in some custom goal, but in undergoal (e.g. only 1 chunkserver up,
# when goal is 2, or 3 cs up, when goal is ec32).
# Then we check if it will replicate to other chunkservers
# and be readable after restarting and upgrading all LizardFS services.

# Variables passed from place where this templated is invoked:
# - GOAL (e.g. 2, ec21, ec32)
# - CHUNKSERVERS_N (compatible with GOAL, e.g. goal 2 -> 2, ec32 -> 5)
# - CHUNKSERVERS_DOWN_N (how many we turn off, e.g. GOAL ec32 -> 2, ec21 -> 1)
# - MOOSEFS_CHUNK_FORMAT ("1" when old chunk format, "0" otherwise)

CHUNKSERVERS_UP_N=$((CHUNKSERVERS_N - CHUNKSERVERS_DOWN_N))
REPLICATION_TIMEOUT="3 minutes"

source test_utils/upgrade.sh

export LZFS_MOUNT_COMMAND="mfsmount"
CHUNKSERVERS=$CHUNKSERVERS_N
	START_WITH_LEGACY_LIZARDFS=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_EXTRA_CONFIG="CREATE_NEW_CHUNKS_IN_MOOSEFS_FORMAT = $MOOSEFS_CHUNK_FORMAT" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_MIN_TIME = 1|OPERATIONS_DELAY_INIT = 0" \
	setup_local_empty_lizardfs info

# Ensure that we work on legacy version, with proper number of services
assert_lizardfsXX_services_count_equals 1 1 $CHUNKSERVERS_N

# Stop CHUNKSERVER_DOWN_N chunkservers, which will result in files with undergoal
stop_lizardfsXX_chunkservers_from_to $CHUNKSERVERS_UP_N $CHUNKSERVERS_N
assert_lizardfsXX_services_count_equals 1 1 $CHUNKSERVERS_UP_N

cd "${info[mount0]}"
mkdir dir
assert_success lizardfsXX mfssetgoal $GOAL dir
cd dir

# map filesize -> cnt, e.g. 20M -> 5 (5 files of filesize 20M)
declare -A filesizes_map
# map filesize -> chunk_nr, e.g. 20M -> 1, 100M -> 2
declare -A chunks_n_map
fill_files_info_maps_with_default_values filesizes_map chunks_n_map # always that, or/and add some custom values to arrays here

# Generate files and check if they are OK
assert_success generate_files_various_filesizes filesizes_map
assert_success check_all_files_readable_and_proper_parts_nr filesizes_map chunks_n_map $CHUNKSERVERS_UP_N

echo "Files properly created. Stopping legacy services."
cd "$TEMP_DIR"
# Stop all legacy services
stop_lizardfsXX_services 1 1 $CHUNKSERVERS_UP_N

# Start recent-version services, all chunkservers now
start_lizardfs_services 1 1 $CHUNKSERVERS_N

# Ensure that versions are switched
assert_no_lizardfsXX_services_active

cd "${info[mount0]}/dir"
# Check if all files will get replicated to all chunkservers
assert_success wait_for_files_replication filesizes_map chunks_n_map "$CHUNKSERVERS_N" "$REPLICATION_TIMEOUT"

# Test if files are readable with all CS on
assert_success check_all_files_readable_and_proper_parts_nr filesizes_map chunks_n_map $CHUNKSERVERS_N

# ADD SLEEP HERE
# sleep 25m

# Check if we can still read files with only minimal chunkservers' nr on
# a) Without first n chunkservers [0, CHUNKSERVERS_DOWN_N)
lizardfs_chunkservers_from_to stop 0 $CHUNKSERVERS_DOWN_N
assert_success check_all_files_readable_and_proper_parts_nr filesizes_map chunks_n_map $CHUNKSERVERS_UP_N
echo "Files OK, without first x chunkservers."

# b) Without last n chunkservers [x, CHUNKSERVERS_N)
lizardfs_chunkservers_from_to start 0 $CHUNKSERVERS_DOWN_N
lizardfs_wait_for_all_ready_chunkservers
lizardfs_chunkservers_from_to stop $CHUNKSERVERS_UP_N $CHUNKSERVERS_N
assert_success check_all_files_readable_and_proper_parts_nr filesizes_map chunks_n_map $CHUNKSERVERS_UP_N
echo "Files OK, without last x chunkservers"
