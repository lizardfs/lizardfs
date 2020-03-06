timeout_set 3 minutes

master_cfg="MAGIC_DISABLE_METADATA_DUMPS = 1"
master_cfg+="|AUTO_RECOVERY = 1"
master_cfg+="|EMPTY_TRASH_PERIOD = 1"
master_cfg+="|EMPTY_RESERVED_INODES_PERIOD = 1"

CHUNKSERVERS=3 \
	MOUNTS=2 \
	USE_RAMDISK="YES" \
	MOUNT_0_EXTRA_CONFIG="mfscachemode=NEVER,mfsreportreservedperiod=1,mfsdirentrycacheto=0" \
	MOUNT_1_EXTRA_CONFIG="mfsmeta" \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota,ignoregid" \
	MFSEXPORTS_META_EXTRA_OPTIONS="nonrootmeta" \
	MASTER_EXTRA_CONFIG="$master_cfg" \
	DEBUG_LOG_FAIL_ON="master.fs.checksum.mismatch" \
	setup_local_empty_lizardfs info

# Save path of meta-mount in MFS_META_MOUNT_PATH for metadata generators
export MFS_META_MOUNT_PATH=${info[mount1]}

# Save path of changelog.mfs in CHANGELOG to make it possible to verify generated changes
export CHANGELOG="${info[master_data_path]}"/changelog.mfs

lizardfs_metalogger_daemon start

# Generate some metadata and remember it
cd "${info[mount0]}"
metadata_generate_all
metadata=$(metadata_print)

# Check if the metadata checksum is fine.
# Possible checksum mismatch will be reported at the end of the test.
assert_success lizardfs_admin_master magic-recalculate-metadata-checksum

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
assert_no_diff "$metadata" "$(metadata_print)"
lizardfs_wait_for_all_ready_chunkservers
metadata_validate_files
