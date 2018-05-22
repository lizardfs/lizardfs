timeout_set 1 minute

master_cfg="MAGIC_DISABLE_METADATA_DUMPS = 1"
master_cfg+="|OPERATIONS_DELAY_INIT = 1"
master_cfg+="|CHUNKS_LOOP_TIME = 1|CHUNKS_LOOP_MAX_CPU = 90"
master_cfg+="|BACK_META_KEEP_PREVIOUS = 0"

CHUNKSERVERS=3 \
	MASTERSERVERS=2 \
	MOUNTS=2 \
	USE_RAMDISK="YES" \
	MOUNT_0_EXTRA_CONFIG="mfsacl,mfscachemode=NEVER,mfsreportreservedperiod=1,mfsdirentrycacheto=0" \
	MOUNT_1_EXTRA_CONFIG="mfsmeta" \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota,ignoregid" \
	MFSEXPORTS_META_EXTRA_OPTIONS="nonrootmeta" \
	MASTER_0_EXTRA_CONFIG="$master_cfg" \
	DEBUG_LOG_FAIL_ON="master.matoml_changelog_apply_error" \
	setup_local_empty_lizardfs info

# Save these two paths for metadata generators
export MFS_META_MOUNT_PATH=${info[mount1]}
export CHANGELOG="${info[master0_data_path]}"/changelog.mfs

# Generate a lot of different changes
cd "${info[mount0]}"
metadata_generate_all
cd

# Dump metadata in the master server and wait for it to finish
assert_success lizardfs_admin_master save-metadata

# There should be no other metadata files (BACK_META_KEEP_PREVIOUS = 0)
assert_equals 1 $(ls "${info[master_data_path]}" | grep -v lock | grep -c metadata)

# Verify if we can start shadow master from the freshly dumped metadata file
lizardfs_master_n 1 start
assert_eventually 'lizardfs_shadow_synchronized 1'

# Verify if we can modify the filesystem and all the changes would be applied by the shadow master
cd "${info[mount0]}"
rm -rf * || true
assert_success grep -q CHECKSUM "$CHANGELOG"
assert_eventually 'lizardfs_shadow_synchronized 1'
