timeout_set 2 minutes

master_cfg="MAGIC_DISABLE_METADATA_DUMPS = 1"

CHUNKSERVERS=3 \
	MASTERSERVERS=2 \
	MOUNTS=2 \
	USE_RAMDISK="YES" \
	MOUNT_0_EXTRA_CONFIG="mfscachemode=NEVER,mfsreportreservedperiod=1,mfsdirentrycacheto=0" \
	MOUNT_1_EXTRA_CONFIG="mfsmeta" \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota,ignoregid" \
	MFSEXPORTS_META_EXTRA_OPTIONS="nonrootmeta" \
	MASTER_EXTRA_CONFIG="$master_cfg" \
	setup_local_empty_lizardfs info

# Save path of meta-mount in MFS_META_MOUNT_PATH for metadata generators
export MFS_META_MOUNT_PATH=${info[mount1]}

# Save path of changelog.mfs in CHANGELOG to make it possible to verify generated changes
export CHANGELOG="${info[master_data_path]}"/changelog.mfs

lizardfs_master_n 1 start

# Generate some metadata and remember it
cd "${info[mount0]}"
metadata_generate_all
metadata=$(metadata_print)
cd

# simulate master server failure and recovery from shadow
assert_eventually "lizardfs_shadow_synchronized 1"
lizardfs_master_daemon kill

lizardfs_make_conf_for_master 1
lizardfs_master_daemon reload
lizardfs_wait_for_all_ready_chunkservers

# check restored filesystem
cd "${info[mount0]}"
assert_no_diff "$metadata" "$(metadata_print)"
metadata_validate_files
