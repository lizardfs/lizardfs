timeout_set 12 minutes

CHUNKSERVERS=2 \
	USE_RAMDISK=YES \
	MASTERSERVERS=2 \
	LZFS_MOUNT_COMMAND="mfsmount" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_1_EXTRA_CONFIG="CREATE_NEW_CHUNKS_IN_MOOSEFS_FORMAT = 0" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1|OPERATIONS_DELAY_INIT = 0" \
	setup_local_empty_lizardfs info

REPLICATION_TIMEOUT='30 seconds'

cd "${info[mount0]}"
# Ensure that we work on legacy version
assert_equals $(lizardfs_admin_master info | grep $LIZARDFSXX_TAG | wc -l) 1

mkdir dir
assert_success lizardfsXX mfssetgoal 2 dir
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
# Check if setgoal/getgoal still work:
assert_success mkdir dir
for goal in {1..9}; do
	assert_equals "dir: $goal" "$(lizardfsXX mfssetgoal "$goal" dir || echo FAILED)"
	assert_equals "dir: $goal" "$(lizardfsXX mfsgetgoal dir || echo FAILED)"
	expected=" files with goal        $goal :          1"
	assert_equals "$expected" "$(lizardfsXX mfsgetgoal -r dir || echo FAILED)"
done

# Check if replication from old LizardFS CS (chunkserver) to LizardFS CS works:
lizardfsXX_chunkserver_daemon 1 stop
assert_success generate_file file1
assert_success file-validate file1
lizardfs_chunkserver_daemon 1 start
assert_eventually \
		'[[ $(lizardfsXX mfscheckfile file1 | grep "chunks with 2 copies" | wc -l) == 1 ]]' "$REPLICATION_TIMEOUT"
lizardfsXX_chunkserver_daemon 0 stop
# Check if LizardFS CS can serve newly replicated chunks to old LizardFS client:
assert_success file-validate file1

# Replication from LizardFS CS to old LizardFS CS is not guaranteed, but writes are supported
lizardfsXX_chunkserver_daemon 0 start
lizardfs_wait_for_all_ready_chunkservers
assert_success generate_file file2
assert_success file-validate file2

# Check if LizardFS CS and old LizardFS CS can communicate with each other when writing a file
# with goal = 2.
# Produce many files in order to test both chunkservers order during write:
many=5
for i in $(seq $many); do
	assert_success generate_file file3_$i
done
# Check if new files can be read both from Moose and from Lizard CS:
lizardfsXX_chunkserver_daemon 0 stop
for i in $(seq $many); do
	assert_success file-validate file3_$i
done
lizardfsXX_chunkserver_daemon 0 start
lizardfs_chunkserver_daemon 1 stop
lizardfs_wait_for_ready_chunkservers 1
for i in $(seq $many); do
	assert_success file-validate file3_$i
done
lizardfs_chunkserver_daemon 1 start
lizardfs_wait_for_all_ready_chunkservers

# Replace old LizardFS CS with LizardFS CS and test the client upgrade:
lizardfsXX_chunkserver_daemon 0 stop
lizardfs_chunkserver_daemon 0 start
lizardfs_wait_for_ready_chunkservers 1
cd "$TEMP_DIR"
# Unmount old LizardFS client:
assert_success lizardfs_mount_unmount 0
# Mount LizardFS client:
assert_success lizardfs_mount_start 0
cd -
# Test if all files produced so far are readable:
assert_success file-validate file0
assert_success file-validate file1
assert_success file-validate file2
for i in $(seq $many); do
	assert_success file-validate file3_$i
done
