timeout_set 4 minutes

CHUNKSERVERS=2 \
	USE_RAMDISK="YES" \
	setup_local_empty_lizardfs info

cd ${info[mount0]}
mkdir hardlink_test
cd hardlink_test
lizardfs settrashtime -r 0 .

# Create a chain of hardlinks
FILE_SIZE=123 file-generate file0
for i in {0..62}; do
	ln file$i file$((i+1))
	assert_equals $(inode_of file$i) $(inode_of file$((i+1)))
	unlink file$i
done

# Check that file is still readable
assert_success file-validate file63

# Check forced hardlinking
for i in {0..4}; do
	ln -f file63 file63-backup
	assert_equals $(inode_of file63) $(inode_of file63-backup)
done

assert_success file-validate file63-backup
assert_success diff file63{,-backup}

# Check large amount of links. Restart master server couple of times to force
# reloading metadata from disk
for i in {0..4098}; do
	ln file63 file63-$i
	assert_equals $(inode_of file63) $(inode_of file63-$i)
	if ! ((i % 1024)); then
		assert_success lizardfs_master_daemon restart
	fi
done

# Restart master and see if everything was loaded correctly
assert_success lizardfs_master_daemon restart

for i in {0..4098}; do
	assert_success file-validate file63-$i
	rm file63-$i
done
