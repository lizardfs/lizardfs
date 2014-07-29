timeout_set 4 minutes

CHUNKSERVERS=2 \
	USE_RAMDISK=YES \
	USE_MOOSEFS=YES \
	AUTO_SHADOW_MASTER=NO \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1|REPLICATIONS_DELAY_INIT = 0" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
mkdir dir
mfssetgoal 2 dir
cd dir

# Start the test with master, two chunkservers and mount running MooseFS code

function generate_file {
	FILE_SIZE=12345678 BLOCK_SIZE=12345 file-generate $1
}

# Test if reading and writing on MooseFS works:
assert_success generate_file file0
assert_success file-validate file0

# Replace MooseFS master with LizardFS master:
lizardfs_master_daemon restart
lizardfs_wait_for_all_ready_chunkservers
# Check if files can still be read:
assert_success file-validate file0

# Check if replication from MooseFS CS (chunkserver) to LizardFS CS works:
moosefs_chunkserver_daemon 1 stop
assert_success generate_file file1
assert_success file-validate file1
lizardfs_chunkserver_daemon 1 start
assert_eventually \
		'[[ $(mfscheckfile file1 | grep "chunks with 2 copies" | wc -l) == 1 ]]' '20 seconds'
moosefs_chunkserver_daemon 0 stop
# Check if LizardFS CS can serve newly replicated chunks to MooseFS client:
assert_success file-validate file1

# Check if replication from LizardFS CS to MooseFS CS works:
assert_success generate_file file2
assert_success file-validate file2
moosefs_chunkserver_daemon 0 start
assert_eventually '[[ $(mfscheckfile file2 | grep "chunks with 2 copies" | wc -l) == 1 ]]' '20 seconds'
lizardfs_chunkserver_daemon 1 stop
# Check if MooseFS CS can serve newly replicated chunks (check if the file is consistent):
assert_success file-validate file2
lizardfs_chunkserver_daemon 1 start
lizardfs_wait_for_all_ready_chunkservers

# Check if LizardFS CS and MooseFS CS can communicate with each other when writing a file
# with goal = 2.
# Produce many files in order to test both chunkservers order during write:
many=5
for i in $(seq $many); do
	assert_success generate_file file3_$i
done
# Check if new files can be read both from Moose and from Lizard CS:
moosefs_chunkserver_daemon 0 stop
for i in $(seq $many); do
	assert_success file-validate file3_$i
done
moosefs_chunkserver_daemon 0 start
lizardfs_chunkserver_daemon 1 stop
lizardfs_wait_for_ready_chunkservers 1
for i in $(seq $many); do
	assert_success file-validate file3_$i
done
lizardfs_chunkserver_daemon 1 start
lizardfs_wait_for_all_ready_chunkservers

# Replace MooseFS CS with LizardFS CS and test the client upgrade:
moosefs_chunkserver_daemon 0 stop
lizardfs_chunkserver_daemon 0 start
lizardfs_wait_for_ready_chunkservers 1
cd "$TEMP_DIR"
# Unmount MooseFS client:
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
