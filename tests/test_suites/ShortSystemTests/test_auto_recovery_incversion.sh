master_cfg="MAGIC_DISABLE_METADATA_DUMPS = 1"
master_cfg+="|AUTO_RECOVERY = 1"
master_cfg+="|DISABLE_METADATA_CHECKSUM_VERIFICATION = 1"

CHUNKSERVERS=2 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota" \
	CHUNKSERVER_EXTRA_CONFIG="HDD_TEST_FREQ = 10000" \
	MASTER_EXTRA_CONFIG="$master_cfg" \
	setup_local_empty_lizardfs info

# Remember version of the metadata file. We expect it not to change when generating data.
metadata_version=$(metadata_get_version "${info[master_data_path]}"/metadata.mfs)

cd ${info[mount0]}
mkdir dir
lizardfs setgoal 2 dir
echo 'aaaaaaaa' > dir/file
assert_equals 1 $(find_chunkserver_chunks 0 | wc -l)
assert_equals 1 $(find_chunkserver_chunks 1 | wc -l)

# Remove chunk from chunkserver 0
chunk=$(find_chunkserver_chunks 0 -name "chunk_0000000000000001_00000001.???")
assert_success rm "$chunk"

# Truncate file (this will generate INCVERSION change) and remember the metadata
truncate -s 1 dir/file
assert_awk_finds '/INCVERSION/' "$(cat "${info[master_data_path]}"/changelog.mfs)"
echo b > something_more  # To make sure that after INCVERSION we are able to apply other changes
metadata=$(metadata_print)

# Simulate crash of the master
cd
lizardfs_master_daemon kill

# Make sure changes are in changelog only (ie. that metadata wasn't dumped)
assert_equals "$metadata_version" "$(metadata_get_version "${info[master_data_path]}"/metadata.mfs)"

# Restore the filesystem from changelog by starting master server and check it
assert_success lizardfs_master_daemon start
lizardfs_wait_for_all_ready_chunkservers
cd "${info[mount0]}"
assert_no_diff "$metadata" "$(metadata_print)"
assert_equals "a" "$(cat dir/file)"
