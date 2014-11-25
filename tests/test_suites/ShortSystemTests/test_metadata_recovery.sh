timeout_set 3 minutes

master_cfg="MAGIC_DISABLE_METADATA_DUMPS = 1"
master_cfg+="|AUTO_RECOVERY = 1"
master_cfg+="|EMPTY_TRASH_PERIOD = 1"
master_cfg+="|EMPTY_RESERVED_INODES_PERIOD = 1"
master_cfg+="|RECALCULATE_CHECKSUM_ON_RELOAD = 1"
master_cfg+="|MAGIC_DEBUG_LOG = master.fs.checksum:$TEMP_DIR/log"

CHUNKSERVERS=3 \
	MOUNTS=2 \
	USE_RAMDISK="YES" \
	MOUNT_0_EXTRA_CONFIG="lfsacl,lfscachemode=NEVER,lfsreportreservedperiod=1" \
	MOUNT_1_EXTRA_CONFIG="lfsmeta" \
	LFSEXPORTS_EXTRA_OPTIONS="allcanchangequota,ignoregid" \
	LFSEXPORTS_META_EXTRA_OPTIONS="nonrootmeta" \
	MASTER_EXTRA_CONFIG="$master_cfg" \
	DEBUG_LOG_FAIL_ON="master.fs.checksum.mismatch" \
	setup_local_empty_lizardfs info

# Save path of meta-mount in LFS_META_MOUNT_PATH for metadata generators
export LFS_META_MOUNT_PATH=${info[mount1]}

# Save path of changelog.lfs in CHANGELOG to make it possible to verify generated changes
export CHANGELOG="${info[master_data_path]}"/changelog.lfs

lizardfs_metalogger_daemon start

# Generate some metadata and remember it
cd "${info[mount0]}"
metadata_generate_all
metadata=$(metadata_print)

# Check if the metadata checksum is fine.
# Possible checksum mismatch will be reported at the end of the test.
lizardfs_master_daemon reload
assert_eventually 'egrep "master.fs.checksum.updater_end" "$TEMP_DIR/log"'

# simulate master server failure and recovery
sleep 3
cd
lizardfs_master_daemon kill
# leave only files written by metalogger
rm ${info[master_data_path]}/{changelog,metadata,sessions}.*
lfsmetarestore -a -d "${info[master_data_path]}"
lizardfs_master_daemon start

# check restored filesystem
cd "${info[mount0]}"
assert_no_diff "$metadata" "$(metadata_print)"
lizardfs_wait_for_all_ready_chunkservers
metadata_validate_files
