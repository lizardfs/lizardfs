cat > $TEMP_DIR/mitmmr1 << END
#!/bin/bash
mfsmetarestore "\$@" | tee $TEMP_DIR/metaout
exit \${PIPESTATUS[0]}
END

MASTER_VAR_PATH=$TEMP_DIR/mfs/var/master

cp $TEMP_DIR/mitmmr1 $TEMP_DIR/mitmmr
chmod a+x $TEMP_DIR/mitmmr

CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="MFSMETARESTORE_PATH = $TEMP_DIR/mitmmr|DUMP_METADATA_ON_RELOAD = 1|PREFER_BACKGROUND_DUMP = 1" \
	setup_local_empty_lizardfs info

# hack: metadata restoration doesn't work on fresh installations
lizardfs_master_daemon restart

# find metadata version
cat > $TEMP_DIR/metadata_version << END
#!/bin/bash
path=$MASTER_VAR_PATH/metadata.mfs.back
mfsmetadump \$path | sed '2q;d' | sed -r 's/.*version: ([0-9]+).*/\1/'
END
chmod +x $TEMP_DIR/metadata_version

# check if the dump was successful
function check() {
	rm -f $TEMP_DIR/metaout

	# changelog.0.mfs exits
	assert_success test -s $MASTER_VAR_PATH/changelog.0.mfs
	last_changelog_entry=$(tail -1 $MASTER_VAR_PATH/changelog.0.mfs | cut -d : -f 1)
	assert_success test -n $last_changelog_entry

	lizardfs_master_daemon reload
	assert_success wait_for "[[ -s $TEMP_DIR/metaout ]]" '2 seconds'
	actual_output=$(cat $TEMP_DIR/metaout)
	assert_equals "${1:-OK}" "$actual_output"

	# check if metadata is up to date
	metadata_version=$($TEMP_DIR/metadata_version)
	assert_success test -n $last_changelog_entry
	assert_success test -n $metadata_version
	echo 1 $last_changelog_entry
	echo 2 $metadata_version
	mfsmetadump $MASTER_VAR_PATH/metadata.mfs.back | head -2
	assert_equals $metadata_version $((last_changelog_entry + 1))
}

function check_no_metarestore() {
	rm -f $TEMP_DIR/metaout
	# changelog.0.mfs exists
	assert_success test -s $MASTER_VAR_PATH/changelog.0.mfs
	# find changelog's last change
	last_changelog_entry=$(tail -1 $MASTER_VAR_PATH/changelog.0.mfs | cut -d : -f 1)
	# master dumps metadata itself
	lizardfs_master_daemon reload
	# check if metadata version is up to date
	find_metadata_cmd="metadata_version=\$($TEMP_DIR/metadata_version)"
	goal="$find_metadata_cmd; ((metadata_version == $((last_changelog_entry + 1))))"
	assert_success wait_for "$goal" '2 seconds'
	# no metout
	assert_success test ! -s $TEMP_DIR/metaout
	# no changelog.0.mfs
	assert_success test ! -s .../changelog.0.mfs
	assert_success test -n $last_changelog_entry
	assert_success test -n $metadata_version
	echo 3 $last_changelog_entry
	echo 4 $metadata_version
	assert_equals $metadata_version $((last_changelog_entry + 1))
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

# Special cases:
# 1. metarestore checksum mismatches (let's assume that checksum 0 is always an error)
cat > $TEMP_DIR/mitmmr2 << END
#!/bin/bash
mfsmetarestore "\$@" -k 0 | tee $TEMP_DIR/metaout
exit \${PIPESTATUS[0]}
END
cp $TEMP_DIR/mitmmr2 $TEMP_DIR/mitmmr

mkdir dir1
touch dir1/file{0..9}
ln dir1/file0 dir1/file0_link
ln -s dir1/file0 dir1/file0_symlink
check ERR
mkfifo dir1/fifo
rm dir1/file0
echo 'abc' > dir1/abc
check_no_metarestore

# now master should try using metarestore
cp $TEMP_DIR/mitmmr1 $TEMP_DIR/mitmmr
dd if=/dev/urandom of=random_file bs=1M count=100
rm -r dir1
check

# 2. metarestore doesn't respond
cat > $TEMP_DIR/mitmmr3 << END
#!/bin/bash
echo 'no response' > $TEMP_DIR/metaout
exit 1
END
cp $TEMP_DIR/mitmmr3 $TEMP_DIR/mitmmr

mkdir dir{0..9}
touch dir{0..9}/file{0..9}
check 'no response'
rm -r dir{5..9}
mv dir{1..4} dir0
cp -r dir0 dir1
check_no_metarestore

# 3. We don't want background dump
CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="MFSMETARESTORE_PATH = $TEMP_DIR/mitmmr|DUMP_METADATA_ON_RELOAD = 1|PREFER_BACKGROUND_DUMP = 1" \
	setup_local_empty_lizardfs info

# hack: metadata restoration doesn't work on fresh installations
lizardfs_master_daemon restart

# cat > $TEMP_DIR/mitmmr4 << END
# #!/bin/bash
# logger 'THIS SHOULD NEVER BE SEEN'
# exit 1
# END
# cp $TEMP_DIR/mitmmr4 $TEMP_DIR/mitmmr
#
# rm -r dir{0,1}
# echo 'foo bar' > 'foo bar'
# check_no_metarestore
