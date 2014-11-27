CHUNKSERVERS=1 \
	USE_RAMDISK="YES" \
	MASTER_EXTRA_CONFIG="METADATA_CHECKSUM_FREQUENCY = 1" \
	setup_local_empty_lizardfs info

changelog_file="${info[master_data_path]}/changelog.lfs"

# Create some metadata
cd ${info[mount0]}
touch file{00..99}
cd

# Make all CHECKSUM entries in changelog incorrect
lizardfs_master_daemon kill
sed -i -e 's/: ./: /' "$changelog_file" # Remove first digit from all timestamps (subtract 37 years)

# Make sure ordinary lfsmetarestore fails, and with disabled checksums succeeds.
assert_failure lfsmetarestore -a -d "${info[master_data_path]}"
assert_success lfsmetarestore -z -a -d "${info[master_data_path]}"
