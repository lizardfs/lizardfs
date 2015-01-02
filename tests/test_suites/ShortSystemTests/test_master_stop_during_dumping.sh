CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="MFSMETARESTORE_PATH = $TEMP_DIR/restore.sh|PREFER_BACKGROUND_DUMP = 1" \
	setup_local_empty_lizardfs info

cat > $TEMP_DIR/restore.sh << END
#!/bin/bash
touch $TEMP_DIR/dump_started
sleep 5
mfsmetarestore "\$@"
END

chmod +x $TEMP_DIR/restore.sh

touch "${info[mount0]}"/file

# begin dumping
assert_success lizardfs_admin_master save-metadata --async
assert_eventually 'test -e $TEMP_DIR/dump_started'

# before dumping ends, stop the server - it should succeed
lizardfs_master_daemon stop
