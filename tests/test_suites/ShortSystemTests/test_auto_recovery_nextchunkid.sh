master_cfg="AUTO_RECOVERY = 1"
master_cfg+="|MAGIC_DISABLE_METADATA_DUMPS = 1"
master_cfg+="|DISABLE_METADATA_CHECKSUM_VERIFICATION = 1"

CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	LFSEXPORTS_EXTRA_OPTIONS="allcanchangequota" \
	MASTER_EXTRA_CONFIG="$master_cfg" \
	AUTO_SHADOW_MASTER="NO" \
	setup_local_empty_lizardfs info

# Create 6 chunks, saving the changelog after generating 3 of them
cd "${info[mount0]}"
FILE_SIZE=1K file-generate {1..3}
cp "${info[master_data_path]}"/changelog.lfs "$TEMP_DIR"
FILE_SIZE=1K file-generate {4..6}
cd

# Lose information about metadata of chunks 4..6
lizardfs_master_daemon kill
mv "$TEMP_DIR"/changelog.lfs "${info[master_data_path]}"

# Start the master - expect it to generate NEXTCHUNKID when chunks 4..6 are registered
lizardfs_master_daemon start
lizardfs_wait_for_all_ready_chunkservers

# Create a new chunk and check if it's number is as high as expected
cd "${info[mount0]}"
FILE_SIZE=1K file-generate 7
assert_awk_finds 0000000000000007 "$(lfsfileinfo 7)"
metadata=$(metadata_print)
cd

# Simulate crash of the master server, auto recover metadata applying NEXTCHUNKID and check it
lizardfs_master_daemon kill
assert_awk_finds '/NEXTCHUNKID/' "$(cat "${info[master_data_path]}"/changelog.lfs)"
assert_success lizardfs_master_daemon start
lizardfs_wait_for_all_ready_chunkservers
assert_no_diff "$metadata" "$(metadata_print "${info[mount0]}")"
