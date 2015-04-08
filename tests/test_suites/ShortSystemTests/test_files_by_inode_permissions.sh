USE_RAMDISK=YES \
	setup_local_empty_lizardfs info


# Create files as lizardfstest user
cd "${info[mount0]}"
mkdir "${info[mount0]}/dir"
for size in {1,2,4,8,16}M; do
	FILE_SIZE="$size" assert_success file-generate "${info[mount0]}/dir/file_$size"
done

# Change permissions so some of the files will be restricted
chmod 440 "dir/file_1M" # inode 3
chmod 220 "dir/file_2M" # inode 4
chmod 000 "dir/file_4M" # inode 5
chmod 660 "dir/file_8M" # inode 6
chmod 770 "dir/file_16M" # inode 7

# Check permissions for files accessed by inode
INODE_PATH="${info[mount0]}/.lizardfs_file_by_inode"
assert_success file-validate "$INODE_PATH/3"
assert_failure dd if=/dev/random of="$INODE_PATH/3" bs=1 count=1
assert_failure file-validate "$INODE_PATH/4"
assert_failure dd if=/dev/random of="$INODE_PATH/5" bs=1 count=1
assert_failure file-validate "$INODE_PATH/5"

# Verify that the fully accessible file is also valid
assert_success file-validate "$INODE_PATH/6"
assert_success dd if=/dev/random of="$INODE_PATH/6" bs=1 count=1

# Verify if execution is permitted
assert_success printf "#/bin/bash\necho 'Hello!\n' > /dev/null" > "$INODE_PATH/7"
assert_success "$INODE_PATH/7"

# ls on .lizardfs_file_by_inode should fail, or we can end up
# with very long listing.
assert_failure ls $INODE_PATH
