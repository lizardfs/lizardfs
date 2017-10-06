CHUNKSERVERS=1 \
	USE_RAMDISK="YES" \
	MASTER_EXTRA_CONFIG="MAGIC_AUTO_FILE_REPAIR = 1"`
			`"|MAGIC_DEBUG_LOG = $TEMP_DIR/log|LOG_FLUSH_ON=DEBUG" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

file1="${info[mount0]}/file1"
file2="${info[mount0]}/file2"
hdd=$(cat "${info[chunkserver0_hdd]}")

# Generate chunks with version 1 and backup them
FILE_SIZE=1K file-generate "$file1" "$file2"
assert_equals 2 $(find_all_chunks | grep '_00000001.' | wc -l)
lizardfs_chunkserver_daemon 0 stop
cp -a "$hdd" "$hdd"_copy
lizardfs_chunkserver_daemon 0 start
lizardfs_wait_for_all_ready_chunkservers

# Change version of chunks to 2
file-overwrite "$file1" "$file2" # Increase version of chunks (the chunkserver was restarted)
MESSAGE="Check if version did increase" \
		assert_equals 2 $(find_all_chunks | grep '_00000002.' | wc -l)

# Revert chunkserver's chunks from the backup
lizardfs_chunkserver_daemon 0 stop
mv "$hdd" "$hdd"_garbage
mv "$hdd"_copy "$hdd"
lizardfs_chunkserver_daemon 0 start
lizardfs_wait_for_all_ready_chunkservers

# Test the MAGIC_AUTO_FILE_REPAIR mechanism
touch "$TEMP_DIR/log"            # Create empty if not exists
file-overwrite "$file1"          # This will result in repairing the chunk from file1
assert_equals 1 $(egrep 'master.fs.file_auto_repaired: [0-9]+ 1' "$TEMP_DIR/log" | wc -l)
file-validate  "$file1" "$file2" # This will result in repairing the chunk from file2
assert_equals 2 $(egrep 'master.fs.file_auto_repaired: [0-9]+ 1' "$TEMP_DIR/log" | wc -l)
assert_equals 2 $(egrep 'master.fs.file_auto_repaired' "$TEMP_DIR/log" | wc -l)

