CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="lfscachemode=NEVER" \
	setup_local_empty_lizardfs info

get_status() {
	"$@" >&2 | true
	echo ${PIPESTATUS[0]}
}

# Test exit statuses of the chunkserver
expect_less_or_equal 2 $(get_status lfschunkserver -c "$TEMP_DIR/nonexistent_file" start)
expect_less_or_equal 2 $(get_status lfschunkserver -c "${info[chunkserver0_config]}" wrongusage)
expect_equals 0 $(get_status lfschunkserver -c "${info[chunkserver0_config]}" isalive)
expect_equals 0 $(get_status lfschunkserver -c "${info[chunkserver0_config]}" restart)
expect_equals 0 $(get_status lfschunkserver -c "${info[chunkserver0_config]}" isalive)
expect_equals 0 $(get_status lfschunkserver -c "${info[chunkserver0_config]}" stop)
expect_equals 0 $(get_status lfschunkserver -c "${info[chunkserver0_config]}" stop)
expect_equals 1 $(get_status lfschunkserver -c "${info[chunkserver0_config]}" isalive)
expect_equals 0 $(get_status lfschunkserver -c "${info[chunkserver0_config]}" start)
expect_equals 0 $(get_status lfschunkserver -c "${info[chunkserver0_config]}" isalive)

# Test exit statuses of the master server
expect_less_or_equal 2 $(get_status lizardfs_master_daemon some_typo)
expect_less_or_equal 2 $(get_status lizardfs_master_daemon -@ restart)
expect_equals 0 $(get_status lizardfs_master_daemon isalive)
expect_equals 0 $(get_status lizardfs_master_daemon restart)
expect_equals 0 $(get_status lizardfs_master_daemon isalive)
expect_equals 0 $(get_status lizardfs_master_daemon stop)
expect_equals 0 $(get_status lizardfs_master_daemon stop)
expect_equals 1 $(get_status lizardfs_master_daemon isalive)
mv "${info[master_data_path]}/metadata.lfs" "${info[master_data_path]}/metadata.lfs.xxx"
expect_less_or_equal 2 $(get_status lizardfs_master_daemon start)
expect_less_or_equal 2 $(get_status lizardfs_master_daemon restart)
expect_equals 1 $(get_status lizardfs_master_daemon isalive)
mv "${info[master_data_path]}/metadata.lfs.xxx" "${info[master_data_path]}/metadata.lfs"
expect_less_or_equal 2 $(get_status lizardfs_master_daemon start)
expect_equals 1 $(get_status lizardfs_master_daemon isalive)
lfsmetarestore -a -d ${info[master_data_path]}
expect_equals 0 $(get_status lizardfs_master_daemon start)
expect_equals 0 $(get_status lizardfs_master_daemon isalive)
