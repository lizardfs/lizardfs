master_extra_config="MFSMETARESTORE_PATH = $TEMP_DIR/metarestore.sh"
master_extra_config+="|DUMP_METADATA_ON_RELOAD = 1"
master_extra_config+="|PREFER_BACKGROUND_DUMP = 1"

CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="$master_extra_config" \
	setup_local_empty_lizardfs info

# hack: metadata restoration doesn't work on fresh installations
lizardfs_master_daemon restart

# 'metaout_tmp' is used to ensure 'metaout' is complete when "created"
cat > $TEMP_DIR/metarestore.sh << END
#!/bin/bash
touch $TEMP_DIR/dump_started
sleep 5
mfsmetarestore "\$@"
END

chmod +x $TEMP_DIR/metarestore.sh

touch "${info[mount0]}"/file

# begin dumping
lizardfs_master_daemon reload
wait_for 'test -e $TEMP_DIR/dump_started' '15 seconds'
# before dumping ends, stop the server - it should succeed
lizardfs_master_daemon stop
