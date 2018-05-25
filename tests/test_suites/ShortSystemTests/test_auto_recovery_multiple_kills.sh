timeout_set 10 minutes

master_cfg="MAGIC_DISABLE_METADATA_DUMPS = 1"

CHUNKSERVERS=1 \
	MOUNTS=2 \
	USE_RAMDISK="YES" \
	MOUNT_0_EXTRA_CONFIG="mfsacl,mfscachemode=NEVER,mfsreportreservedperiod=1,mfsdirentrycacheto=0" \
	MOUNT_1_EXTRA_CONFIG="mfsmeta" \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota,ignoregid" \
	MFSEXPORTS_META_EXTRA_OPTIONS="nonrootmeta" \
	MASTER_EXTRA_CONFIG="$master_cfg" \
	setup_local_empty_lizardfs info

# Save path of meta-mount in MFS_META_MOUNT_PATH for metadata generators
export MFS_META_MOUNT_PATH=${info[mount1]}

# Save path of changelog.mfs in CHANGELOG to make it possible to verify generated changes
export CHANGELOG="${info[master_data_path]}/changelog.mfs"

for generator in $(metadata_get_all_generators); do
	export MESSAGE="Testing generator $generator"

	# Remember version of the metadata file. We expect it not to change when generating data.
	metadata_version=$(metadata_get_version "${info[master_data_path]}"/metadata.mfs)

	# Generate some content using the current generator and remember its metadata
	cd "${info[mount0]}"
	eval "$generator"
	metadata=$(metadata_print)
	cd

	# Simulate crash of the master server
	lizardfs_master_daemon kill

	# Make sure changes are in changelog only (ie. that metadata wasn't dumped)
	assert_equals "$metadata_version" "$(metadata_get_version "${info[master_data_path]}"/metadata.mfs)"

	# Restore the filesystem from changelog by starting master server and check it
	assert_failure lizardfs_master_daemon start # Should fail without -o auto-recovery!
	assert_success lizardfs_master_daemon start -o auto-recovery
	lizardfs_wait_for_all_ready_chunkservers
	assert_no_diff "$metadata" "$(metadata_print "${info[mount0]}")"
done

# Check if we can read files
cd "${info[mount0]}"
metadata_validate_files
