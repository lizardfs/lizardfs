timeout_set 45 seconds

# Test checks if both legacy, and new LizardFS mount
# work with legacy versions of master and chunkservers

CHUNKSERVERS=2 \
	MOUNTS=2 \
	START_WITH_LEGACY_LIZARDFS=YES \
	LZFS_MOUNT_COMMAND="mfsmount" \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_1_EXTRA_CONFIG="CREATE_NEW_CHUNKS_IN_MOOSEFS_FORMAT = 0" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1|OPERATIONS_DELAY_INIT = 0" \
	setup_local_empty_lizardfs info

# Start test with master, 2 chunkservers and 2 mounts running legacy LizardFS code
# Ensure that we work on legacy version
assert_equals 1 $(lizardfs_admin_master info | grep $LIZARDFSXX_TAG | wc -l)
assert_equals 2 $(lizardfs_admin_master list-chunkservers | grep $LIZARDFSXX_TAG | wc -l)
assert_equals 2 $(lizardfs_admin_master list-mounts | grep $LIZARDFSXX_TAG | wc -l)

cd "${info[mount0]}"
mkdir dir0
assert_success lizardfsXX mfssetgoal 2 dir0
cd dir0

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
