timeout_set 1 minute

# We create file in goal '2', in undergoal (only 1 chunkserver on),
# Then we check if it will replicate and be readable after restarting
# and upgrading all LizardFS services.

CHUNKSERVERS=2
	USE_RAMDISK=YES \
	START_WITH_LEGACY_LIZARDFS=YES \
	# LZFS_MOUNT_COMMAND="mfsmount" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_EXTRA_CONFIG="CREATE_NEW_CHUNKS_IN_MOOSEFS_FORMAT = $MOOSEFS_CHUNK_FORMAT" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_MIN_TIME = 1|OPERATIONS_DELAY_INIT = 0" \
	setup_local_empty_lizardfs info

REPLICATION_TIMEOUT='30 seconds'

# Start test with master, 2 chunkservers and mount running old LizardFS code
# Ensure that we work on legacy version
assert_equals 1 $(lizardfs_admin_master info | grep $LIZARDFSXX_TAG | wc -l)
assert_equals 2 $(lizardfs_admin_master list-chunkservers | grep $LIZARDFSXX_TAG | wc -l)
assert_equals 1 $(lizardfs_admin_master list-mounts | grep $LIZARDFSXX_TAG | wc -l)

cd "${info[mount0]}"

# Leave just 1 chunkserver on, which will result in undergoal
assert_success lizardfsXX_chunkserver_daemon 1 stop
mkdir dir
assert_success lizardfsXX mfssetgoal 2 dir
cd dir

# Start the test with master, one chunkserver and mount running old LizardFS code
function generate_file {
	FILE_SIZE=12345678 BLOCK_SIZE=12345 file-generate $1
}

assert_success generate_file file0
assert_success file-validate file0

# Test if there's just 1 copy
assert_equals 1 $(lizardfs checkfile file0 | grep 'with 1 copy:' | wc -l)

# Restart all services, start in new version
cd "$TEMP_DIR"
lizardfsXX_chunkserver_daemon 0 stop
lizardfs_mount_unmount 0
lizardfs_master_daemon restart
lizardfs_mount_start 0
lizardfs_chunkserver_daemon 0 start
lizardfs_chunkserver_daemon 1 start
lizardfs_wait_for_all_ready_chunkservers

# Ensure that versions are switched
assert_equals 0 $(lizardfs_admin_master info | grep $LIZARDFSXX_TAG | wc -l)
assert_equals 0 $(lizardfs_admin_master list-chunkservers | grep $LIZARDFSXX_TAG | wc -l)
assert_equals 0 $(lizardfs_admin master list-mounts | grep $LIZARDFSXX_TAG | wc -l)

# Test if file is still readable
cd "${info[mount0]}/dir"
assert_success file-validate file0

# Check if it'll replicate to second chunkserver
assert_eventually '[[ $(lizardfs checkfile file0 | grep "chunks with 2 copies" | wc -l) == 1 ]]' \
	"$REPLICATION_TIMEOUT"

# Check if we can still read it with only 1 chunkserver on
lizardfs_chunkserver_daemon 0 stop
assert_success file-validate file0

# And with another
lizardfs_chunkserver_daemon 1 stop
lizardfs_chunkserver_daemon 0 start
assert_success file-validate file0
