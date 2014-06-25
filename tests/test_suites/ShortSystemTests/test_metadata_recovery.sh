timeout_set 2 minutes

CHUNKSERVERS=3 \
	MOUNTS=2 \
	USE_RAMDISK="YES" \
	MOUNT_0_EXTRA_CONFIG="mfsacl,mfscachemode=NEVER" \
	MOUNT_1_EXTRA_CONFIG="mfsmeta" \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota,ignoregid" \
	MFSEXPORTS_META_EXTRA_OPTIONS="nonrootmeta" \
	setup_local_empty_lizardfs info

# Save path of meta-mount in MFS_META_MOUNT_PATH for metadata generators
export MFS_META_MOUNT_PATH=${info[mount1]}

lizardfs_metalogger_daemon start

# Generate some metadata and remember it
cd "${info[mount0]}"
metadata_generate_all
metadata=$(metadata_print)

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
metadata_validate_files
