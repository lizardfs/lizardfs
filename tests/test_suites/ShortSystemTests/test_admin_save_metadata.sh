master_cfg="MFSMETARESTORE_PATH = $TEMP_DIR/metarestore.sh"
master_cfg+="|MAGIC_PREFER_BACKGROUND_DUMP = 1"
master_cfg+="|BACK_META_KEEP_PREVIOUS = 50"
master_cfg+="|MAGIC_DISABLE_METADATA_DUMPS = 1"

CHUNKSERVERS=1 \
	USE_RAMDISK="YES" \
	MASTER_EXTRA_CONFIG="$master_cfg" \
	ADMIN_PASSWORD="pass" \
	setup_local_empty_lizardfs info
port=${lizardfs_info_[matocl]}

# Instead of real mfsmetarestore, provide a program which hangs forever to slow down metadata dumps
cat > "$TEMP_DIR/metarestore.sh" << END
#!/bin/sh
touch "$TEMP_DIR/dump_started"
sleep 4
mfsmetarestore "\$@" || exit $?
touch "$TEMP_DIR/dump_finished"
END
chmod +x "$TEMP_DIR/metarestore.sh"

# A helper function which counts copies of metadata
count_metadata_files() {
	( cd "${info[master_data_path]}" ; ls metadata* | grep -v '[.]lock' | grep -v tmp | wc -l )
}
expect_equals 2 $(count_metadata_files) # 'MFSM NEW' and it's binary form

# Verify if wrong password doesn't work
touch "${info[mount0]}/file1"  # To make changelog not empty for metarestore
rm -f "$TEMP_DIR"/dump_*
assert_failure lizardfs-admin save-metadata localhost "$port" <<< "no-pass"
assert_equals 2 $(count_metadata_files)
assert_file_not_exists "$TEMP_DIR/dump_started"

# Verify if the command without --async blocks us until metadata is created
touch "${info[mount0]}/file2"  # To make changelog not empty for metarestore
rm -f "$TEMP_DIR"/dump_*
assert_success lizardfs-admin save-metadata localhost "$port" <<< "pass"
assert_file_exists "$TEMP_DIR/dump_finished"
assert_equals 3 $(count_metadata_files)

# Verify if the command with --async starts the process, but doesn't block us
rm -f $TEMP_DIR/dump_*
touch "${info[mount0]}/file3"  # To make changelog not empty for metarestore
assert_success lizardfs-admin save-metadata localhost "$port" --async <<< "pass"
assert_file_not_exists "$TEMP_DIR/dump_finished"
assert_equals 3 $(count_metadata_files)

# Verify if the command fails if a dump is in progress
touch "${info[mount0]}/file4"  # To make changelog not empty for metarestore
assert_failure lizardfs-admin save-metadata localhost "$port" --async <<< "pass"
assert_failure lizardfs-admin save-metadata localhost "$port" <<< "pass"
assert_equals 3 $(count_metadata_files)

# Verify if the async dump eventually finishes
assert_eventually_prints 4 'count_metadata_files'

# Verify if save-metadata properly reports status of the operation (using metarestore)
chmod -w "${info[master_data_path]}"  # Make it impossible to save metadata
assert_failure lizardfs-admin save-metadata localhost "$port" <<< "pass"
chmod +w "${info[master_data_path]}"  # Fix data dir

# Verify if save-metadata properly reports status of the operation (using fork)
sed -i -re "s/(MAGIC_PREFER_BACKGROUND_DUMP).*/\1 = 0/" "${info[master_cfg]}"
lizardfs_admin_master reload-config
assert_success lizardfs-admin save-metadata localhost "$port" <<< "pass"
chmod -w "${info[master_data_path]}"  # Make it impossible to save metadata
assert_failure lizardfs-admin save-metadata localhost "$port" <<< "pass"
chmod +w "${info[master_data_path]}"  # Fix data dir
