USE_RAMDISK=YES \
	MASTER_EXTRA_CONFIG="DUMP_METADATA_ON_RELOAD = 1|MAGIC_DISABLE_METADATA_DUMPS = 1" \
	setup_local_empty_lizardfs info
lizardfs_metalogger_daemon start

# Prints md5 hashes of all master's and metalogger's changelog files.
# changelog_ml.lfs.1 and changelog_ml.lfs.2 are omitted, because metalogger
# overwrites these two when starting and we don't want a race in:
#   lizardfs_metalogger_daemon start
#   assert_no_diff "$expected_changelogs" "$(changelog_checksums)"
changelog_checksums() {
	md5sum changelog*.lfs* | grep -v '_ml[.]lfs[.][12]' | sort
}

metadata_file="${info[master_data_path]}/metadata.lfs"
cd "${info[mount0]}"
# Generate some changelog files in master and metalogger
for n in {1..10}; do
	touch file_${n}.{1..10}
	prev_version=$(metadata_get_version "$metadata_file")
	lizardfs_master_daemon reload
	assert_eventually '(( $(metadata_get_version "$metadata_file") > prev_version ))'
	assert_eventually "test -f '${info[master_data_path]}/changelog.lfs.$n'"
done

cd ${info[master_data_path]}
lizardfs_master_daemon stop
lizardfs_metalogger_daemon stop
echo 111 > changelog.lfs
echo kazik > changelog_ml.lfs
expected_changelogs=$(changelog_checksums)

# Rename changelog files so they simulate old version
for i in {1..99}; do
	if [[ -e changelog.lfs.$i ]]; then
		mv changelog.lfs.$i changelog.${i}.lfs
	fi
	if [[ -e changelog_ml.lfs.$i ]]; then
		mv changelog_ml.lfs.$i changelog_ml.${i}.lfs
	fi
done
mv changelog.lfs changelog.0.lfs
mv changelog_ml.lfs changelog_ml.0.lfs
assert_not_equal "$expected_changelogs" "$(changelog_checksums)"

# Start master and metalogger, and make sure they properly rename changelog files
lizardfs_master_daemon start
lizardfs_metalogger_daemon start
assert_no_diff "$expected_changelogs" "$(changelog_checksums)"
