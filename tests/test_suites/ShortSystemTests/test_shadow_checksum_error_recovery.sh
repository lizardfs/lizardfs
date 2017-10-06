CHUNKSERVERS=1 \
	MASTERSERVERS=2 \
	USE_RAMDISK="YES" \
	MASTER_EXTRA_CONFIG="MAGIC_DISABLE_METADATA_DUMPS = 1 | MAGIC_DEBUG_LOG = ${TEMP_DIR}/log`
	`|LOG_FLUSH_ON=DEBUG" \
	setup_local_empty_lizardfs info

touch "${info[mount0]}"/file

# Corrupt the changelog, start a shadow master and see if it can deal with it.
sed -i 's/file/fool/g' "${info[master_data_path]}"/changelog.mfs
lizardfs_master_n 1 start
assert_eventually "lizardfs_shadow_synchronized 1"

# Check if the shadow stays synchronized after the error recovery.
touch "${info[mount0]}"/filefilefile
assert_eventually "lizardfs_shadow_synchronized 1"

# Verify that it worked the way we expected, not due to bugs or a blind luck.
assert_success awk '
	/master.mismatch/ && i == 0 {i++; next;}
	/master.mltoma_changelog_apply_error/ && i == 1 {i++; next;}
	/master.matoml_changelog_apply_error/ && i == 2 {i++; next;}
	END {if (i != 3) {exit 1}}
	' ${TEMP_DIR}/log
