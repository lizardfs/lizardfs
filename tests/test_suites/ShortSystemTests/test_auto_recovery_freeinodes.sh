timeout_set 1 minute

master_cfg="MAGIC_DISABLE_METADATA_DUMPS = 1"
master_cfg+="|AUTO_RECOVERY = 1"
master_cfg+="|FREE_INODES_PERIOD = 1"
master_cfg+="|DISABLE_METADATA_CHECKSUM_VERIFICATION = 1"

CHUNKSERVERS=1 \
	MOUNTS=1 \
	USE_RAMDISK="YES" \
	MOUNT_0_EXTRA_CONFIG="mfscachemode=NEVER,mfsattrcacheto=0,mfsreportreservedperiod=1" \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota" \
	MASTER_EXTRA_CONFIG="$master_cfg" \
	AUTO_SHADOW_MASTER="NO" \
	setup_local_empty_lizardfs info

changelog_file="${info[master_data_path]}/changelog.mfs"
metadata_file="${info[master_data_path]}/metadata.mfs"

# Remember version of the metadata file. We expect it not to change when generating data.
metadata_version=$(metadata_get_version "$metadata_file")

# Create and remove inodes, which later will be freed generating FREEINODES entry in changelog
cd ${info[mount0]}
touch file{00..99}
assert_eventually '[[ $(grep RELEASE "$changelog_file" | wc -l) == 100 ]]'
lizardfs settrashtime 0 file*
rm file{10..80}
cd

# Decrease timestamp for all operations in changelog by a factor of at least 24h
lizardfs_master_daemon kill
assert_equals "$metadata_version" "$(metadata_get_version "$metadata_file")"
sed -i -e 's/: ./: /' "$changelog_file" # Remove first digit from all timestamps (subtract 37 years)

# Start the master server (it will recover and dump metadata) and remember version of the new file.
assert_success lizardfs_master_daemon start
lizardfs_wait_for_all_ready_chunkservers
new_metadata_version=$(metadata_get_version "$metadata_file")
assert_less_than "$metadata_version" "$new_metadata_version"

if [ -f "$changelog_file" ]; then
	assert_awk_finds_no '/CREATE.*:20$/' "$(cat "$changelog_file")"
fi

touch "${info[mount0]}"/file{0000..099}
assert_awk_finds    '/CREATE.*:20$/' "$(cat "$changelog_file")" # Make sure that inode 20 was reused

# Simulate crash of the master server, auto recover metadata applying FREEINODES and check it
metadata=$(metadata_print "${info[mount0]}")
lizardfs_master_daemon kill
assert_equals "$new_metadata_version" "$(metadata_get_version "$metadata_file")"
assert_success lizardfs_master_daemon start
lizardfs_wait_for_all_ready_chunkservers
assert_no_diff "$metadata" "$(metadata_print "${info[mount0]}")"
