master_cfg="MAGIC_DISABLE_METADATA_DUMPS = 1"
master_cfg+="|METADATA_CHECKSUM_RECALCULATION_SPEED = 1"
master_cfg+="|MAGIC_DEBUG_LOG = $TEMP_DIR/log|LOG_FLUSH_ON=DEBUG"
touch "$TEMP_DIR/log"

CHUNKSERVERS=1 \
	USE_RAMDISK="YES" \
	MASTER_EXTRA_CONFIG="$master_cfg" \
	ADMIN_PASSWORD="pass" \
	setup_local_empty_lizardfs info
port=${lizardfs_info_[matocl]}

# Create a lot of metadata
cd ${info[mount0]}
if valgrind_enabled; then
	mkdir {1..1000}
else
	mkdir {1..8000}
fi


# Verify if wrong password doesn't work
assert_failure lizardfs-admin magic-recalculate-metadata-checksum localhost "$port" <<< "no-pass"

assert_equals 0 $(grep updater_end "$TEMP_DIR/log" | wc -l)
assert_equals 0 $(grep updater_start "$TEMP_DIR/log" | wc -l)

# Verify if the command without --async blocks us until checkum is recalculated
time assert_success lizardfs-admin magic-recalculate-metadata-checksum localhost "$port" <<< "pass"
log_data=$(tail -n 100 "$TEMP_DIR/log")
assert_equals 1 $(echo "$log_data" | grep updater_end | wc -l)
assert_equals 1 $(echo "$log_data" | grep updater_start | wc -l)

# Verify if the command with --async starts the process, but doesn't block us
time assert_success lizardfs-admin magic-recalculate-metadata-checksum localhost "$port" --async <<< "pass"
log_data=$(tail -n 100 "$TEMP_DIR/log")
assert_equals 2 $(echo "$log_data" | grep updater_start | wc -l)
assert_equals 1 $(echo "$log_data" | grep updater_end | wc -l)
assert_eventually_prints 2 'grep updater_end "$TEMP_DIR/log" | wc -l'
