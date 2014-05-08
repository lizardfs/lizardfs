timeout_set 90 seconds

master_extra_config="MFSMETARESTORE_PATH = $TEMP_DIR/metarestore.sh"
master_extra_config+="|DUMP_METADATA_ON_RELOAD = 1"
master_extra_config+="|PREFER_BACKGROUND_DUMP = 1"
master_extra_config+="|BACK_META_KEEP_PREVIOUS = 5"

CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG=$master_extra_config \
	setup_local_empty_lizardfs info

# hack: metadata restoration doesn't work on fresh installations
lizardfs_master_daemon restart

# 'metaout_tmp' is used to ensure 'metaout' is complete when "created"
cat > $TEMP_DIR/metarestore_ok.sh << END
#!/bin/bash
mfsmetarestore "\$@" | tee $TEMP_DIR/metaout_tmp
ret="\${PIPESTATUS[0]}"
mv $TEMP_DIR/metaout_tmp $TEMP_DIR/metaout
exit "\$ret"
END

cat > $TEMP_DIR/metarestore_wrong_checksum.sh << END
#!/bin/bash
mfsmetarestore "\$@" -k 0 | tee $TEMP_DIR/metaout_tmp
ret="\${PIPESTATUS[0]}"
mv $TEMP_DIR/metaout_tmp $TEMP_DIR/metaout
exit "\$ret"
END

cat > $TEMP_DIR/metarestore_no_response.sh << END
#!/bin/bash
echo 'no response' > $TEMP_DIR/metaout_tmp
mv $TEMP_DIR/metaout_tmp $TEMP_DIR/metaout
exit 1
END

cat > $TEMP_DIR/metarestore_error_if_executed.sh << END
#!/bin/bash
echo 'THIS SHOULD NEVER BE SEEN' > $TEMP_DIR/metaout_tmp
mv $TEMP_DIR/metaout_tmp $TEMP_DIR/metaout
exit 1
END

cp $TEMP_DIR/metarestore_ok.sh $TEMP_DIR/metarestore.sh
chmod a+x $TEMP_DIR/metarestore.sh

backup_copies=1   # First backup was created during the initial restart (aka hack)
function check_backup_copies() {
	expect_equals $backup_copies $(ls "${info[master_data_path]}"/metadata.mfs.back.? | wc -l)
	expect_file_exists "${info[master_data_path]}/metadata.mfs.back"
	for (( i = 1 ; i <= backup_copies ; ++i )); do
		expect_file_exists "${info[master_data_path]}/metadata.mfs.back.$i"
	done
}

# check <master|metarestore> <OK|ERR>
# dumps metadata and checks results
function check() {
	cd "${info[master_data_path]}"
	rm -f "$TEMP_DIR/metaout"
	assert_file_exists "changelog.0.mfs"
	assert_file_exists "metadata.mfs.back"

	prev_metadata_inode=$(stat --format=%i metadata.mfs.back)
	lizardfs_master_daemon reload
	if [[ $2 == OK ]]; then
		# wait for dump to be finished
		metadata_is_dumped='[[ $(stat --format=%i metadata.mfs.back.1) == $prev_metadata_inode ]]'
		files_are_renamed='[[ -e metadata.mfs.back && ! -e metadata.mfs.back.tmp ]]'
		assert_success wait_for "$metadata_is_dumped" '10 seconds'
		assert_success wait_for "$files_are_renamed" '10 seconds'
	fi

	# verify if metadata was or was not used
	if [[ $1 == metarestore ]]; then
		assert_success wait_for 'test -e $TEMP_DIR/metaout' '10 seconds'
	else
		assert_file_not_exists "$TEMP_DIR/metaout"
	fi

	if [[ $2 == OK ]]; then
		# check if the dumped metadata is up to date,
		# ie. if its version is equal to (1 + last entry in changelog.1)
		assert_file_exists changelog.1.mfs
		last_change=$(tail -1 changelog.1.mfs | cut -d : -f 1)
		assert_success test -n "$last_change"
		assert_equals $((last_change+1)) "$(mfsmetadump metadata.mfs.back | awk 'NR==2{print $6}')"
		if ((backup_copies < 5)); then
			backup_copies=$((backup_copies + 1))
		fi
	fi
	check_backup_copies
	cd -
}

cd "${info[mount0]}"

FILE_SIZE=200B file-generate to_be_destroyed
mfsfilerepair to_be_destroyed
check metarestore OK

csid=$(find_first_chunkserver_with_chunks_matching '*.mfs')
mfschunkserver -c "${info[chunkserver${csid}_config]}" stop
lizardfs_wait_for_ready_chunkservers 2
mfsfilerepair to_be_destroyed
check metarestore OK

while read command; do
	eval "$command"
	MESSAGE="testing $command" check metarestore OK
done <<'END'
touch file1
attr -s attr1 -V '' file1
setfattr -n user.attr2 -v 'some value' file1
setfattr -x user.attr1 file1
setfattr -n user.attr1 -v 'different value' file1
attr -s attr2 -V 'not the same, I am sure' file1
attr -r attr2 file1
mkdir dir
touch dir/file1 dir/file2
mkfifo fifo
touch file
ln file link
ln -s file symlink
mv file file2
ln -fs file2 symlink
echo 'abc' > symlink
touch file{00..99}
mfssettrashtime 0 file1{0..4}
rm file1?
mv file99 file999
mfssetgoal 3 file999
mfssetgoal 9 file03
head -c 1M < /dev/urandom > random_file
mfssettrashtime 3 random_file
truncate -s 100M random_file
head -c 1M < /dev/urandom > random_file2
truncate -s 100 random_file2
truncate -s 1T sparse
head -c 16M /dev/urandom | dd seek=1 bs=127M conv=notrunc of=sparse
head -c 1M /dev/urandom >> sparse
mfsmakesnapshot sparse sparse2
head -c 16M /dev/urandom | dd seek=1 bs=127M conv=notrunc of=sparse2
truncate -s 1000M sparse2
truncate -s 100 sparse2
truncate -s 0 sparse2
mfsmakesnapshot -o random_file random_file2
head -c 2M /dev/urandom | dd seek=1 bs=1M conv=notrunc of=random_file
truncate -s 1000M sparse
truncate -s 100 sparse
truncate -s 0 sparse
rm sparse
setfacl -d -m group:fuse:rw- dir
setfacl -d -m user:lizardfstest:rwx dir
setfacl -m group:fuse:rw- dir/file1
setfacl -m group:adm:rwx dir/file1
touch dir/aclfile
setfacl -m group::r-x dir/aclfile
setfacl -x group:fuse dir/aclfile
setfacl -k dir
setfacl -b dir/aclfile
setfacl -m group:fuse:rw- dir
setfacl -m group:fuse:rw- dir/aclfile
END

# Special cases:
# 1. metarestore checksum mismatches (let's assume that checksum 0 is always an error)
cp $TEMP_DIR/metarestore_wrong_checksum.sh $TEMP_DIR/metarestore.sh

mkdir dir1
touch dir1/file{0..9}
ln dir1/file0 dir1/file0_link
ln -s dir1/file0 dir1/file0_symlink
check metarestore ERR

mkfifo dir1/fifo
rm dir1/file0
echo 'abc' > dir1/abc
check master OK

# now master should try using metarestore
cp $TEMP_DIR/metarestore_ok.sh $TEMP_DIR/metarestore.sh

head -c 1M < /dev/urandom > u_ran_doom
rm -r dir1
check metarestore OK

# 2. metarestore doesn't respond
cp $TEMP_DIR/metarestore_no_response.sh $TEMP_DIR/metarestore.sh

mkdir dir{0..9}
touch dir{0..9}/file{0..9}
check metarestore ERR
assert_equals "no response" "$(cat $TEMP_DIR/metaout)"

rm -r dir{5..9}
mv dir{1..4} dir0
cp -r dir0 dir1
check master OK

# 3. We don't want background dump
sed -ie 's/PREFER_BACKGROUND_DUMP = 1/PREFER_BACKGROUND_DUMP = 0/' "${lizardfs_info[master_cfg]}"

cp $TEMP_DIR/metarestore_error_if_executed.sh $TEMP_DIR/metarestore.sh
mkdir dir{11..22}
echo 'abc' | tee dir{12..21}/file{0..9}
echo 'foo bar' > 'foo bar'
check master OK
