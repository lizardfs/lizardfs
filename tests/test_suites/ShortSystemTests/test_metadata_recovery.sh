assert_program_installed attr

CHUNKSERVERS=3 \
	setup_local_empty_lizardfs info

oldmeta="${TEMP_DIR}/old_metadata"
newmeta="${TEMP_DIR}/new_metadata"

# hack: metadata restoration doesn't work on fresh installations
lizardfs_master_daemon restart
lizardfs_metalogger_daemon start

cd "${info[mount0]}"

# create some funny inodes
mkdir dir
touch dir/file1 dir/file2
mkfifo fifo
touch file
ln file link
ln -s file symlink

# create more files and delete some of them
touch file{00..99}
rm file1?

# use mfssetgoal / mfssettrashtime
for goal in 1 2 3 4 ; do
	touch goal$goal
	mfssetgoal $goal goal$goal
done
for trashtime in 123 234 345 ; do
	touch trashtime$trashtime
	mfssettrashtime $trashtime trashtime$trashtime
done

# create few nonempty files and chunks
echo xxxx >x
echo yyyy >y
echo zzzz >z

# test sharing and "unsharing" chunks
mfsappendchunks xyz x y z
truncate -s2 x
echo 'zZzZ' >z

# test snapshotting
mfsmakesnapshot dir dir-snapshot

# set and remove some xattrs
attr -qs name1 -V value1 file
attr -qs name2 -V value2 file
attr -qs name3 -V value3 dir
attr -r name1 file

# set and remove some ACLs
mkdir acldir
setfacl -d -m group:fuse:rw- acldir
setfacl -d -m user:lizardfstest:rwx acldir
touch acldir/aclfile
setfacl -m group::r-x acldir/aclfile
setfacl -x group:fuse acldir/aclfile
setfacl -m group:root:-wx acldir/aclfile
setfacl -k acldir
setfacl -d -m group:fuse:rwx acldir

# gather and store some metadata
print_metadata() {
	ls -l .
	ls -l dir
	ls -l dir-snapshot
	mfsgetgoal goal*
	mfsgettrashtime trashtime*
	mfsfileinfo x y z xyz | grep -v $'^\t\t'       # remove "copy N" and "no valid copies"
	attr -ql file
	attr -qg name2 file
	attr -ql dir
	attr -qg name3 dir
	getfacl -R .
}
print_metadata >"$oldmeta"

# simulate master server failure and recovery
sleep 3
cd
lizardfs_master_daemon kill
# leave only files written by metalogger
rm ${info[master_data_path]}/{changelog,metadata,sessions}.*
mfsmetarestore -a -d "${info[master_data_path]}"
lizardfs_master_daemon start

# check restored filesystem
cd "${info[mount0]}"
print_metadata >"$newmeta"

diff=$(diff -u "$oldmeta" "$newmeta") || true

if [ -n "$diff" ]; then
	test_add_failure $'Restored metadata different from original:\n'"$diff"
fi
