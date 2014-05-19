timeout_set 2 minutes
assert_program_installed attr

CHUNKSERVERS=3 \
	MOUNT_EXTRA_CONFIG="mfsacl" \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota,ignoregid" \
	setup_local_empty_lizardfs info

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
