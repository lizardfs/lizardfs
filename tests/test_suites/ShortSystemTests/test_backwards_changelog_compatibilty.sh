USE_RAMDISK=YES \
	MASTER_EXTRA_CONFIG="DUMP_METADATA_ON_RELOAD = 1|MAGIC_DISABLE_METADATA_DUMPS = 1" \
	setup_local_empty_lizardfs info

lizardfs_metalogger_daemon start

metadata_file="${info[master_data_path]}/metadata.mfs"
cd "${info[mount0]}"
# Generate some changelog files in master and metalogger
for n in {1..10}; do
	touch file_${n}.{1..10}
	prev_version=$(metadata_get_version "$metadata_file")
	lizardfs_master_daemon reload
	assert_success wait_for '(( $(metadata_get_version "$metadata_file") > prev_version ))' '10 seconds'
	assert_success wait_for "test -f '${info[master_data_path]}/changelog.mfs.$n'" '10 seconds'
done

cd ${info[master_data_path]}
lizardfs_master_daemon stop
lizardfs_metalogger_daemon stop
echo 111 > changelog.mfs
echo kazik > changelog_ml.mfs
expected_changelogs=$(md5sum changelog*.mfs* | sort)

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
assert_not_equal "$expected_changelogs" "$(md5sum changelog*.mfs* | sort)"

# Start master and metalogger, and make sure they properly rename changelog files
lizardfs_master_daemon start
lizardfs_metalogger_daemon start
assert_no_diff "$expected_changelogs" "$(md5sum changelog*.mfs* | sort)"
