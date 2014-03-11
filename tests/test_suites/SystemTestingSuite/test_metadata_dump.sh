CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="MFSMETARESTORE_PATH = $TEMP_DIR/metarestore.sh|DUMP_METADATA_ON_RELOAD = 1|PREFER_BACKGROUND_DUMP = 1" \
	setup_local_empty_lizardfs info

# find metadata version
cat > $TEMP_DIR/metadata_version << END
#!/bin/bash
path=${info[master_data_path]}/metadata.mfs.back
mfsmetadump \$path | sed '2q;d' | sed -r 's/.*version: ([0-9]+).*/\1/'
END
chmod +x $TEMP_DIR/metadata_version

cat > $TEMP_DIR/metarestore_ok.sh << END
#!/bin/bash
mfsmetarestore "\$@" | tee $TEMP_DIR/metaout
exit \${PIPESTATUS[0]}
END

cat > $TEMP_DIR/metarestore_wrong_checksum.sh << END
#!/bin/bash
mfsmetarestore "\$@" -k 0 | tee $TEMP_DIR/metaout
exit \${PIPESTATUS[0]}
END

cat > $TEMP_DIR/metarestore_no_response.sh << END
#!/bin/bash
echo 'no response' > $TEMP_DIR/metaout
exit 1
END

cat > $TEMP_DIR/metarestore_error_if_executed.sh << END
#!/bin/bash
echo 'THIS SHOULD NEVER BE SEEN' > $TEMP_DIR/metaout
exit 1
END

cp $TEMP_DIR/metarestore_ok.sh $TEMP_DIR/metarestore.sh
chmod a+x $TEMP_DIR/metarestore.sh

# hack: metadata restoration doesn't work on fresh installations
lizardfs_master_daemon restart

# check if the dump was successful
function check() {
	rm -f $TEMP_DIR/metaout

	# changelog.0.mfs exits
	assert_success test -s ${info[master_data_path]}/changelog.0.mfs
	last_changelog_entry=$(tail -1 ${info[master_data_path]}/changelog.0.mfs | cut -d : -f 1)
	assert_success test -n $last_changelog_entry

	lizardfs_master_daemon reload
	assert_success wait_for "[[ -s $TEMP_DIR/metaout ]]" '2 seconds'
	actual_output=$(cat $TEMP_DIR/metaout)
	assert_equals "${1:-OK}" "$actual_output"

	if [[ "${1:-OK}" != "ERR" ]]; then
		# check if metadata is up to date
		metadata_version=$($TEMP_DIR/metadata_version)
		assert_success test -n $last_changelog_entry
		assert_success test -n $metadata_version
		assert_equals $metadata_version $((last_changelog_entry + 1))
	fi
}

function check_no_metarestore() {
	rm -f "$TEMP_DIR/metaout"
	# changelog.0.mfs exists
	assert_success test -s "${info[master_data_path]}/changelog.0.mfs"
	# find changelog's last change
	last_changelog_entry=$(tail -1 "${info[master_data_path]}/changelog.0.mfs" | cut -d : -f 1)
	# master dumps metadata itself
	lizardfs_master_daemon reload
	# check if metadata version is up to date
	find_metadata_cmd="metadata_version=\$($TEMP_DIR/metadata_version)"
	goal="$find_metadata_cmd; ((metadata_version == $((last_changelog_entry + 1))))"
	assert_success wait_for "$goal" '2 seconds'
	# no metaout
	assert_failure test -s $TEMP_DIR/metaout
	# no changelog.0.mfs
	assert_failure test -s "${info[master_data_path]}/changelog.0.mfs"
	assert_success test -n $last_changelog_entry
	assert_success test -n $metadata_version
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

mv file99 file999
mfssetgoal 3 file999
mfssetgoal 9 file03
check

head -c 1M < /dev/urandom > random_file
mfssettrashtime 3 random_file
truncate -s 100M random_file
head -c 1M < /dev/urandom > random_file2
truncate -s 100 random_file2
check

truncate -s 1T sparse
head -c 16M /dev/urandom | dd seek=1 bs=127M conv=notrunc of=sparse
head -c 1M /dev/urandom >> sparse
check

mfsmakesnapshot sparse sparse2
head -c 16M /dev/urandom | dd seek=1 bs=127M conv=notrunc of=sparse2
rm sparse
mfsmakesnapshot -o random_file random_file2
head -c 2M /dev/urandom | dd seek=1 bs=1M conv=notrunc of=random_file
check

# Special cases:
# 1. metarestore checksum mismatches (let's assume that checksum 0 is always an error)
cp $TEMP_DIR/metarestore_wrong_checksum.sh $TEMP_DIR/metarestore.sh

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
cp $TEMP_DIR/metarestore_ok.sh $TEMP_DIR/metarestore.sh
head -c 1M < /dev/urandom > u_ran_doom
rm -r dir1
check

# 2. metarestore doesn't respond
cp $TEMP_DIR/metarestore_no_response.sh $TEMP_DIR/metarestore.sh

mkdir dir{0..9}
touch dir{0..9}/file{0..9}

rm -f $TEMP_DIR/metaout
lizardfs_master_daemon reload # metarestore failed, no backup files created
assert_success wait_for "[[ -s $TEMP_DIR/metaout ]]" '2 seconds'
assert_equals "no response" "$(cat $TEMP_DIR/metaout)"

rm -r dir{5..9}
mv dir{1..4} dir0
cp -r dir0 dir1
check_no_metarestore

# 3. We don't want background dump
sed -ie 's/PREFER_BACKGROUND_DUMP = 1/PREFER_BACKGROUND_DUMP = 0/' "${lizardfs_info[master_cfg]}"
lizardfs_master_daemon reload

cp $TEMP_DIR/metarestore_error_if_executed.sh $TEMP_DIR/metarestore.sh
mkdir dir{11..22}
echo 'abc' | tee dir{12..21}/file{0..9}
echo 'foo bar' > 'foo bar'
check_no_metarestore
