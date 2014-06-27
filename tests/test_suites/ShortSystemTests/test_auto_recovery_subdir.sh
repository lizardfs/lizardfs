timeout_set 3 minutes

CHUNKSERVERS=1 \
	MOUNTS=2 \
	USE_RAMDISK="YES" \
	MOUNT_EXTRA_CONFIG="mfsacl,mfscachemode=NEVER" \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota,ignoregid" \
	MASTER_EXTRA_CONFIG="MAGIC_DISABLE_METADATA_DUMPS = 1|AUTO_RECOVERY = 1" \
	setup_local_empty_lizardfs info

# Create subdir and modify mount #1 to use it
mkdir -p "${info[mount0]}/some/subfolder"
chmod 1777 "${info[mount0]}/some/subfolder"
lizardfs_mount_unmount 1
echo "mfssubfolder=some/subfolder" >> "${info[mount1_config]}"
lizardfs_mount_start 1

# Remember version of the metadata file. We expect it not to change when generating data.
metadata_file="${info[master_data_path]}/metadata.mfs"
metadata_version=$(metadata_get_version "$metadata_file")

# Generate metadata in /some/subfolder and save it as seen from /some/subfolder
cd "${info[mount1]}"
assert_equals "" "$(ls)" # some/subfolder should be empty!
for generator in $(metadata_get_all_generators | egrep -v "trash_ops|quota"); do
	eval "$generator"
done
chmod 1775 . # Special operation in this test -- changing attributes of the root inode
metadata_subdir=$(metadata_print)

# Let's also save the metadata seen from root inode
cd "${info[mount0]}"
metadata_root=$(metadata_print)

# Make master server apply all the changelogs using AUTO_RECOVRY feature
cd
lizardfs_master_daemon kill
assert_equals "$metadata_version" "$(metadata_get_version "$metadata_file")"
assert_success lizardfs_master_daemon start
lizardfs_wait_for_all_ready_chunkservers

# Verify the restored metadata
cd "${info[mount0]}"
MESSAGE="Comparing metadata in root" assert_no_diff "$metadata_root" "$(metadata_print)"
cd "${info[mount1]}"
MESSAGE="Comparing metadata in subdir" assert_no_diff "$metadata_subdir" "$(metadata_print)"
metadata_validate_files
