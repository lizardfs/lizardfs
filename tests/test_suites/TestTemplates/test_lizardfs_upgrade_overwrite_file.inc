timeout_set 1 minute

# We create file in goal '2'.
# We close one of two chunkservers, so that this file is in undergoal.
# We overwrite the file, then restart and upgrade all LizardFS services.
# New version should then remain, and be replicated to second chunkserver.

CHUNKSERVERS=2
	USE_RAMDISK=YES \
	START_WITH_LEGACY_LIZARDFS=YES \
	# LZFS_MOUNT_COMMAND="mfsmount" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_EXTRA_CONFIG="CREATE_NEW_CHUNKS_IN_MOOSEFS_FORMAT = $MOOSEFS_CHUNK_FORMAT" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_MIN_TIME = 1|OPERATIONS_DELAY_INIT = 0" \
	setup_local_empty_lizardfs info

REPLICATION_TIMEOUT='30 seconds'

# Start the test with master, two chunkservers and mount running old LizardFS code
# Ensure that we work on legacy version

assert_equals 1 $(lizardfs_admin_master info | grep $LIZARDFSXX_TAG | wc -l)
assert_equals 2 $(lizardfs_admin_master list-chunkservers | grep $LIZARDFSXX_TAG | wc -l)
assert_equals 1 $(lizardfs_admin_master list-mounts | grep $LIZARDFSXX_TAG | wc -l)

cd "${info[mount0]}"
mkdir dir
assert_success lizardfsXX mfssetgoal 2 dir
cd dir

function generate_file {
	FILE_SIZE=12345678 BLOCK_SIZE=12345 file-generate $1
}

assert_success generate_file file0

# Wait until chunk has been replicated
assert_eventually '[[ $(lizardfs checkfile file0 | grep "chunks with 2 copies" | wc -l) == 1 ]]' \
	"$REPLICATION_TIMEOUT"

# We stop 1 chunkserver, and overwrite file
lizardfsXX_chunkserver_daemon 0 stop
new_file_content="12345"
echo $new_file_content > file0
assert_equals $new_file_content $(cat file0)

# Restart all services, start in new version
cd "$TEMP_DIR"
lizardfsXX_chunkserver_daemon 1 stop
lizardfs_mount_unmount 0
lizardfs_master_daemon restart
lizardfs_mount_start 0
lizardfs_chunkserver_daemon 0 start
lizardfs_chunkserver_daemon 1 start

# Ensure that versions are switched
assert_equals 0 $(lizardfs_admin_master info | grep $LIZARDFSXX_TAG | wc -l)
assert_equals 0 $(lizardfs_admin_master list-chunkservers | grep $LIZARDFSXX_TAG | wc -l)
assert_equals 0 $(lizardfs_admin master list-mounts | grep $LIZARDFSXX_TAG | wc -l)

# Check if new content was preserved, not old one. On both chunkservers.
cd "${info[mount0]}/dir"
assert_eventually '[[ $(lizardfs checkfile file0 | grep "chunks with 2 copies" | wc -l) == 1 ]]' \
	"$REPLICATION_TIMEOUT"
assert_equals $new_file_content $(cat file0)

lizardfs_chunkserver_daemon 0 stop
assert_equals $new_file_content $(cat file0)

lizardfs_chunkserver_daemon 1 stop
lizardfs_chunkserver_daemon 0 start
assert_equals $new_file_content $(cat file0)
