CHUNKSERVERS=1 \
	MASTERSERVERS=2 \
	USE_RAMDISK="YES" \
	MASTER_EXTRA_CONFIG="MASTER_TIMEOUT = 10 | MAGIC_DISABLE_METADATA_DUMPS = 1" \
	setup_local_empty_lizardfs info

# Add a shadow master and wait until it's fully synchronized
lizardfs_master_n 1 start
assert_eventually "lizardfs_shadow_synchronized 1"

# Generate some changes and remember a list of files in shadow's working directory
touch "${info[mount0]}"/file{1..10}
assert_eventually "lizardfs_shadow_synchronized 1"
files_before=$(ls "${info[master1_data_path]}" | sort)

# Make shadow master lose connection with the master by making it sleep more than timeout
shadow_pid=$(lizardfs_master_n 1 test 2>&1 | sed 's/.*: //')
assert_matches "^[0-9]+$" "$shadow_pid"
kill -STOP "$shadow_pid"
sleep 13

# Generate some new changes when shadow is sleeping and wake him up
# Expect it to synchronize without downloading any new files
touch "${info[mount0]}"/file{1..20}
kill -CONT "$shadow_pid"
assert_eventually "lizardfs_shadow_synchronized 1"
assert_equals "$files_before" "$(ls "${info[master1_data_path]}" | sort)"
