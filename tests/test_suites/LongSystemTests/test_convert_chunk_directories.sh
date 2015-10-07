timeout_set 40 minutes

LIZARDFSXX_TAG=2.6.0

CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	MASTERSERVERS=1 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1|REPLICATIONS_DELAY_INIT = 0" \
	AUTO_SHADOW_MASTER="NO" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
# Ensure that we work on legacy version
assert_equals $(lizardfs_admin_master info | grep $LIZARDFSXX_TAG | wc -l) 1

mkdir dir
cd dir

# Create 200K chunks
for dname in dir{1..200}; do
	echo Creating files in $dname
	mkdir $dname
	for fname in file{1..1000}; do
		FILE_SIZE=128 assert_success file-generate $dname/$fname
	done
done

# Replace old LizardFS master with LizardFS master:
lizardfs_master_daemon restart
assert_equals $(lizardfs_admin_master info | grep $LIZARDFSXX_TAG | wc -l) 0
lizardfs_wait_for_all_ready_chunkservers

# Stop old chunk server
lizardfsXX_chunkserver_daemon 0 stop

# Make list of all chunks
OLD_CHUNK_LIST=$(find $RAMDISK_DIR/hdd_0_0 -type f -printf "%f\n" | grep chunk_ | sort)

# Start new chunk server
lizardfs_chunkserver_daemon 0 start

# Wait for scan to finish (so chunk server is usable)
assert_eventually "grep 'scanning folder.*complete' $ERROR_DIR/syslog.log" "1 minute"

# Check if chunks are usable during migrate
# Reverse order of the check guarantees that we will access some unmigrated chunks.
for dname in dir{200..190}; do
	echo Testing files in $dname
	for fname in file{1000..1}; do
		assert_success file-validate $dname/$fname
	done
done
assert_failure grep 'converting directories in folder.*complete' $ERROR_DIR/syslog.log

# Check if directory migration stops correctly
lizardfs_chunkserver_daemon 0 stop
assert_eventually "grep 'converting directories in folder.*interrupted' $ERROR_DIR/syslog.log" "1 minute"

lizardfs_chunkserver_daemon 0 start
for dname in dir{189..170}; do
	echo Testing files in $dname
	for fname in file{1000..1}; do
		assert_success file-validate $dname/$fname
	done
done

assert_eventually "grep 'converting directories in folder.*complete' $ERROR_DIR/syslog.log" "5 minutes"

# Make list of all chunks in new catalog layout
NEW_CHUNK_LIST=$(find $RAMDISK_DIR/hdd_0_0/chunk* -type f -printf "%f\n" | sort)

# Check if all chunks were moved
assert_equals "$OLD_CHUNK_LIST" "$NEW_CHUNK_LIST"
