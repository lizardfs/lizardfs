master_cfg="MFSMETARESTORE_PATH = $TEMP_DIR/metarestore.sh"
master_cfg+="|DUMP_METADATA_ON_RELOAD = 1"
master_cfg+="|PREFER_BACKGROUND_DUMP = 1"
master_cfg+="|MAGIC_DISABLE_METADATA_DUMPS = 1"

CHUNKSERVERS=1 \
	MASTERSERVERS=2 \
	USE_RAMDISK="YES" \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota" \
	MOUNT_EXTRA_CONFIG="mfsacl,mfscachemode=NEVER"
	MASTER_EXTRA_CONFIG="$master_cfg" \
	setup_local_empty_lizardfs info

# Instead of real mfsmetarestore, provide a program which hangs forever to slow down metadata dumps
cat > "$TEMP_DIR/metarestore.sh" << END
#!/bin/bash
touch "$TEMP_DIR/dump_started"
sleep 30 # Long enough to do the test, short enough to be able to terminate memcheck within 60s
mfsmetarestore "\$@"
touch "$TEMP_DIR/dump_finished"
END
chmod +x "$TEMP_DIR/metarestore.sh"

# Generate some files
cd "${info[mount0]}"
touch file_before_shadow_start_{1..20}
lizardfs_master_n 1 start                              # Connect shadow master
assert_eventually "lizardfs_shadow_synchronized 1"
touch file_after_shadow_connects_{1..20}
lizardfs_master_n 1 reload                             # Start dumping metadata in shadow master
wait_for 'test -e $TEMP_DIR/dump_started' '15 seconds'
touch file_after_shadow_reload_{1..20}
metadata=$(metadata_print)
cd

# Wait for master and shadow master to synchronize
assert_eventually "lizardfs_shadow_synchronized 1"

# Promote shadow master to master
lizardfs_master_daemon kill
lizardfs_make_conf_for_master 1

# Disable metadata dumping in shadow master
sed -ie 's/DUMP_METADATA_ON_RELOAD = 1/DUMP_METADATA_ON_RELOAD = 0/' "${info[master1_cfg]}"

lizardfs_master_daemon reload
lizardfs_wait_for_all_ready_chunkservers
assert_file_not_exists "$TEMP_DIR/dump_finished"

cd "${info[mount0]}"
assert_no_diff "$metadata" "$(metadata_print)"
