CHUNKSERVERS=3 \
	setup_local_empty_lizardfs info

oldmeta="${TEMP_DIR}/old_metadata"
newmeta="${TEMP_DIR}/new_metadata"

# hack: metadata restoration doesn't work on fresh installations
stop_master_daemon
start_master_daemon
run_metalogger

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

# gather and store some metadata
print_metadata() {
	ls -l .
	ls -l dir
	ls -l dir-snapshot
	mfsgetgoal goal*
	mfsgettrashtime trashtime*
	mfsfileinfo x y z xyz | grep -v $'^\t\t'       # remove "copy N" and "no valid copies"
}
print_metadata >"$oldmeta"

# simulate master server failure and recovery
sleep 3
cd
kill_master_daemon
# leave only files written by metalogger
rm ${info[master_data_path]}/{changelog,metadata,sessions}.*
mfsmetarestore -a -d "${info[master_data_path]}"
start_master_daemon

# check restored filesystem
cd "${info[mount0]}"
print_metadata >"$newmeta"

diff=$(diff -u "$oldmeta" "$newmeta") || true

if [ -n "$diff" ]; then
	test_add_failure $'Restored metadata different from original:\n'"$diff"
fi
