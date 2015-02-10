CHUNKSERVERS=1 \
	USE_RAMDISK="YES" \
	MASTER_EXTRA_CONFIG="MAGIC_DISABLE_METADATA_DUMPS = 1|BACK_META_KEEP_PREVIOUS = 5" \
	setup_local_empty_lizardfs info

changelog_file="${info[master_data_path]}/changelog.mfs"

# Create some metadata
for i in {1..5} ; do
	FILE_SIZE=1K assert_success file-generate "${info[mount0]}"/file_${i}_{1..10}
	assert_success lizardfs_admin_master save-metadata
done

# Test fresh metadata + changelogs
latest_metadata_version=$(lizardfs_probe_master metadataserver-status | cut -f3)
on_disk_metadata_version=$(mfsmetarestore -g -d "${info[master_data_path]}")
assert_equals "$latest_metadata_version" "$on_disk_metadata_version"

# Create more metadata
FILE_SIZE=1K assert_success file-generate "${info[mount0]}"/file_{6..10}_{1..10}
# and don't save it

# Test old metadata + changelogs
latest_metadata_version=$(lizardfs_probe_master metadataserver-status | cut -f3)
lizardfs_master_daemon kill
on_disk_metadata_version=$(mfsmetarestore -g -d "${info[master_data_path]}")
assert_equals "$latest_metadata_version" "$on_disk_metadata_version"
assert_success lizardfs_master_daemon start -o auto-recovery
assert_equals "$(lizardfs_probe_master metadataserver-status | cut -f3)" "$on_disk_metadata_version"

# Test broken changelogs fail
rm "${info[mount0]}"/file_*
lizardfs_master_daemon kill

ls "${info[master_data_path]}"
mv "${info[master_data_path]}"/metadata.mfs.2 "${info[master_data_path]}"/metadata.mfs
mv "${info[master_data_path]}"/changelog.mfs.1 "${info[master_data_path]}"/changelog.mfs
mv "${info[master_data_path]}"/changelog.mfs.2 "${info[master_data_path]}"/changelog.mfs.2.tmp

on_disk_metadata_version=$(mfsmetarestore -g -d "${info[master_data_path]}")
assert_equals "0" "$on_disk_metadata_version"

# Test with changelog and metadata missing (but we should still be able to load old version)
mv "${info[master_data_path]}"/changelog.mfs.2.tmp "${info[master_data_path]}"/changelog.mfs.2
on_disk_metadata_version=$(mfsmetarestore -g -d "${info[master_data_path]}")
assert_success lizardfs_master_daemon start -o auto-recovery
assert_equals "$(lizardfs_probe_master metadataserver-status | cut -f3)" "$on_disk_metadata_version"

# Test mfsmetarestore on clean installation
assert_success lizardfs_master_daemon kill
echo "MFSM NEW" > "${info[master_data_path]}"/metadata.mfs
on_disk_metadata_version=$(mfsmetarestore -g -d "${info[master_data_path]}")
assert_equals "1" "$on_disk_metadata_version"
