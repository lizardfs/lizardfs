cat > $TEMP_DIR/mitmmr << END
#!/bin/bash
mfsmetarestore "\$@" | tee $TEMP_DIR/metaout
exit \${PIPESTATUS[0]}
END

chmod a+x $TEMP_DIR/mitmmr

CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="MFSMETARESTORE_PATH = $TEMP_DIR/mitmmr|DUMP_METADATA_ON_RELOAD = 1|PREFER_BACKGROUND_DUMP = 1" \
	setup_local_empty_lizardfs info

# hack: metadata restoration doesn't work on fresh installations
lizardfs_master_daemon restart

# check if the dump was successful
function check() {
	rm -f $TEMP_DIR/metaout
	lizardfs_master_daemon reload
	assert_success wait_for "[[ -s $TEMP_DIR/metaout ]]" '2 seconds'
	actual_output=$(cat $TEMP_DIR/metaout)
	assert_equals OK "$actual_output"
}

cd "${info[mount0]}"

touch file1
check

mkdir dir
touch dir/file1 dir/file2
check

mkfifo fifo
check

touch file
check

ln file link
ln -s file symlink
check

# create more files and delete some of them
touch file{00..99}
rm file1?
check
