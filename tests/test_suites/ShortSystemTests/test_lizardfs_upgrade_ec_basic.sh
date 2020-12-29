timeout_set 90 seconds

# Creates a file in EC(3, 2) on all-legacy version of LizardFS.
# Then checks if file is still readable after
#   a) updating just master
#   b) updating all services

CHUNKSERVERS=5
	USE_RAMDISK=YES \
	START_WITH_LEGACY_LIZARDFS=YES \
	MASTERSERVERS=2 \
	LZFS_MOUNT_COMMAND="mfsmount" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_1_EXTRA_CONFIG="CREATE_NEW_CHUNKS_IN_MOOSEFS_FORMAT = 0" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_MIN_TIME = 1|OPERATIONS_DELAY_INIT = 0" \
	setup_local_empty_lizardfs info

REPLICATION_TIMEOUT='30 seconds'

# Start test with master, 5 chunkservers and mount running old LizardFS code
# Ensure that we work on legacy version
assert_equals 1 $(lizardfs_admin_master info | grep $LIZARDFSXX_TAG | wc -l)
assert_equals 5 $(lizardfs_admin_master list-chunkservers | grep $LIZARDFSXX_TAG | wc -l)
assert_equals 1 $(lizardfs_admin_master list-mounts | grep $LIZARDFSXX_TAG | wc -l)

cd "${info[mount0]}"
mkdir dir
assert_success lizardfsXX mfssetgoal ec32 dir
cd dir

function generate_file {
	FILE_SIZE=12345678 BLOCK_SIZE=12345 file-generate $1
}

# Test if reading and writing on old LizardFS works:
assert_success generate_file file0
assert_success file-validate file0

# Start shadow
lizardfs_master_n 1 restart
assert_eventually "lizardfs_shadow_synchronized 1"

# Replace old LizardFS master with LizardFS master:
lizardfs_master_daemon restart
# Ensure that versions are switched
assert_equals 0 $(lizardfs_admin_master info | grep $LIZARDFSXX_TAG | wc -l)
lizardfs_wait_for_all_ready_chunkservers
# Check if files can still be read:
assert_success file-validate file0

# Replace old LizardFS CS with LizardFS CS and test the client upgrade:
for i in {0..4}; do
	lizardfsXX_chunkserver_daemon $i stop
	lizardfs_chunkserver_daemon $i start
done
lizardfs_wait_for_all_ready_chunkservers

cd "$TEMP_DIR"
# Unmount old LizardFS client:
assert_success lizardfs_mount_unmount 0
# Mount LizardFS client:
assert_success lizardfs_mount_start 0
cd -
# Test if all files produced so far are readable:
assert_success file-validate file0
