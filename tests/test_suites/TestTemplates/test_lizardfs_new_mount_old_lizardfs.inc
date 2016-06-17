timeout_set 12 minutes

CHUNKSERVERS=2 \
	MOUNTS=2 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_1_EXTRA_CONFIG="CREATE_NEW_CHUNKS_IN_MOOSEFS_FORMAT = 0" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1|REPLICATIONS_DELAY_INIT = 0" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
# Ensure that we work on legacy version
assert_success $(lizardfs_admin_master info | grep -q $LIZARDFSXX_TAG)

mkdir dir0
assert_success lizardfsXX mfssetgoal 2 dir0
cd dir0

# Start the test with master, two chunkservers and mount running old LizardFS code
function generate_file {
	FILE_SIZE=12345678 BLOCK_SIZE=12345 file-generate $1
}

# Test if reading and writing on old LizardFS works:
assert_success generate_file file0
assert_success file-validate file0

# Unmount old LizardFS client 1:
assert_success lizardfs_mount_unmount 1
# Mount LizardFS client 1:
assert_success lizardfs_mount_start 1

cd "${info[mount1]}/dir0"

# Test if file created on legacy version is readable
assert_success file-validate file0

cd ..
mkdir dir1
assert_success lizardfsXX mfssetgoal 2 dir1
cd dir1
# Test if reading and writing on new LizardFS mount works:
assert_success generate_file file1
assert_success file-validate file1

# Test if all files produced so far are readable on legacy mount:
cd "${info[mount0]}/dir0"
assert_success file-validate file0
cd ../dir1
assert_success file-validate file1
