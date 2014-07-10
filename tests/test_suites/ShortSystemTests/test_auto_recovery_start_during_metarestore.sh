CHUNKSERVERS=0 \
	MASTER_EXTRA_CONFIG="AUTO_RECOVERY = 1" \
	MOUNTS=0 \
	setup_local_empty_lizardfs info

# Generate empty metadata file by stopping the master server and generate a long changelog.
lizardfs_master_daemon stop
assert_equals 1 $(metadata_get_version "${info[master_data_path]}/metadata.mfs")
generate_changelog > "${info[master_data_path]}/changelog.mfs"

# Start mfsmetarestore in background and wait until it starts to apply
# the changelog. This process will then last for a couple of seconds.
mfsmetarestore -a -d "${info[master_data_path]}" &
wait_for 'test -e "${info[master_data_path]}/metadata.mfs.lock"' "15 seconds"
sleep 1

# Try to start the master server. This should fail, because mfsmetarestore holds the lock.
expect_failure lizardfs_master_daemon start

# Expect that mfsmetarestore still applies the changelog
expect_equals 1 $(metadata_get_version "${info[master_data_path]}/metadata.mfs")
