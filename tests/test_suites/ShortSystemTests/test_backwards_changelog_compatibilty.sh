USE_RAMDISK=YES \
	MASTER_EXTRA_CONFIG="MAGIC_DISABLE_METADATA_DUMPS = 1" \
	setup_local_empty_lizardfs info
lizardfs_metalogger_daemon start

# Prints md5 hashes of all master's and metalogger's changelog files.
# changelog_ml.mfs.1 and changelog_ml.mfs.2 are omitted, because metalogger
# overwrites these two when starting and we don't want a race in:
#   lizardfs_metalogger_daemon start
#   assert_no_diff "$expected_changelogs" "$(changelog_checksums)"
changelog_checksums() {
	md5sum changelog*.mfs* | grep -v '_ml[.]mfs[.][12]' | sort
}

metadata_file="${info[master_data_path]}/metadata.mfs"
cd "${info[mount0]}"
# Generate some changelog files in master and metalogger
for n in {1..10}; do
	touch file_${n}.{1..10}
	prev_version=$(metadata_get_version "$metadata_file")
	assert_success lizardfs_admin_master save-metadata
	assert_less_than "$prev_version" "$(metadata_get_version "$metadata_file")"
	assert_file_exists "${info[master_data_path]}/changelog.mfs.$n"
done

cd ${info[master_data_path]}
lizardfs_master_daemon stop
lizardfs_metalogger_daemon stop
echo 111 > changelog.mfs
echo kazik > changelog_ml.mfs
expected_changelogs=$(changelog_checksums)

# Rename changelog files so they simulate old version
for i in {1..99}; do
	if [[ -e changelog.mfs.$i ]]; then
		mv changelog.mfs.$i changelog.${i}.mfs
	fi
	if [[ -e changelog_ml.mfs.$i ]]; then
		mv changelog_ml.mfs.$i changelog_ml.${i}.mfs
	fi
done
mv changelog.mfs changelog.0.mfs
mv changelog_ml.mfs changelog_ml.0.mfs
assert_not_equal "$expected_changelogs" "$(changelog_checksums)"

# Start master and metalogger, and make sure they properly rename changelog files
lizardfs_master_daemon start
lizardfs_metalogger_daemon start

sleep 1
assert_no_diff "$expected_changelogs" "$(changelog_checksums)"
