timeout_set 12 minutes

export LIZARDFSXX_TAG=3.11.0

CHUNKSERVERS=5
	USE_RAMDISK=YES \
	MASTERSERVERS=2 \
	LZFS_MOUNT_COMMAND="mfsmount" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_1_EXTRA_CONFIG="CREATE_NEW_CHUNKS_IN_MOOSEFS_FORMAT = 0" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_MIN_TIME = 1|OPERATIONS_DELAY_INIT = 0" \
	setup_local_empty_lizardfs info

REPLICATION_TIMEOUT='30 seconds'

cd "${info[mount0]}"
# Ensure that we work on legacy version
assert_equals $(lizardfs_admin_master info | grep $LIZARDFSXX_TAG | wc -l) 1

mkdir dir
assert_success lizardfsXX mfssetgoal ec32 dir
cd dir

# Start the test with master, two chunkservers and mount running old LizardFS code
function generate_file {
	FILE_SIZE=12345678 BLOCK_SIZE=12345 file-generate $1
}

# Test if reading and writing on old LizardFS works:
assert_success generate_file file0
assert_success file-validate file0

# Start shadows
lizardfs_master_n 1 restart
assert_eventually "lizardfs_shadow_synchronized 1"

# Replace old LizardFS master with LizardFS master:
lizardfs_master_daemon restart
# Ensure that versions are switched
assert_equals $(lizardfs_admin_master info | grep $LIZARDFSXX_TAG | wc -l) 0
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
