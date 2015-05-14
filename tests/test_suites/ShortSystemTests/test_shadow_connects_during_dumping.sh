master_cfg="MFSMETARESTORE_PATH = $TEMP_DIR/metarestore.sh"
master_cfg+="|MAGIC_PREFER_BACKGROUND_DUMP = 1"
master_cfg+="|MAGIC_DISABLE_METADATA_DUMPS = 1"

CHUNKSERVERS=1 \
	MASTERSERVERS=2 \
	USE_RAMDISK="YES" \
	MASTER_EXTRA_CONFIG="$master_cfg" \
	setup_local_empty_lizardfs info

# Instead of real mfsmetarestore, provide a program which hangs forever to slow down metadata dumps
cat > "$TEMP_DIR/metarestore.sh" << END
#!/usr/bin/env bash
touch "$TEMP_DIR/dump_started"
sleep 30 # Long enough to do the test, short enough to be able to terminate memchek within 60 s
touch "$TEMP_DIR/dump_finishing"
END
chmod +x "$TEMP_DIR/metarestore.sh"

# Generate some files
cd "${info[mount0]}"
touch file{1..20}
lizardfs_admin_master save-metadata --async        # Start dumping metadata
assert_eventually 'test -e $TEMP_DIR/dump_started'
touch file{30..40}
lizardfs_master_n 1 start                          # Connect shadow master during the dump
assert_eventually "lizardfs_shadow_synchronized 1"
touch file{50..60}

# Expect them to synchronize despite of the dump in progress
assert_eventually "lizardfs_shadow_synchronized 1"
assert_file_not_exists "$TEMP_DIR/dump_finishing"

# Check if new changes are also being synchronised between metadata servers
rm file{50..60}
assert_eventually "lizardfs_shadow_synchronized 1"
