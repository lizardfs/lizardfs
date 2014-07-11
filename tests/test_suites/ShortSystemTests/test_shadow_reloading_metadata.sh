timeout_set '1 minute'

CHUNKSERVERS=1 \
	MASTERSERVERS=2 \
	USE_RAMDISK="YES" \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota" \
	MASTER_EXTRA_CONFIG="MAGIC_DISABLE_METADATA_DUMPS = 1" \
	setup_local_empty_lizardfs info

m=${info[master0_data_path]} # master's working directory
s=${info[master1_data_path]} # shadow's working directory

# Generate any changes, connect shadow master and wait until it's fully synchronized
# Master should have metadata file with version 1, but after synchronization shadow will dump newer
touch "${info[mount0]}"/file
lizardfs_master_n 1 start
assert_equals 1 "$(metadata_get_version "$m/metadata.mfs")"
assert_eventually '[[ $(metadata_get_version "$s/metadata.mfs") -gt 1 ]]'
shadows_version=$(metadata_get_version "$s/metadata.mfs")

# Make more changes, shadow should apply them
touch "${info[mount0]}"/file{1..10}
assert_eventually 'cmp <(get_changes "$m") <(get_changes "$s") &>/dev/null'

# Make shadow master lose connection with the master by making it sleep and restarting master server
shadow_pid=$(lizardfs_master_n 1 test 2>&1 | sed 's/.*: //')
assert_matches "^[0-9]+$" "$shadow_pid"
kill -STOP "$shadow_pid"
lizardfs_master_daemon restart

# Generate a lot of new changes and remove changelogs from shadow's version to master's version
touch "${info[mount0]}"/file{1..20}
lizardfs_master_daemon stop
rm "$m"/changelog*
lizardfs_master_daemon start
touch "${info[mount0]}"/file{20..30}

# Make shadow master recover it's connection and expect shadow master to synchronize correctly
kill -CONT "$shadow_pid"
assert_eventually 'cmp <(get_changes "$m" | tail) <(get_changes "$s" | tail) &>/dev/null'
assert_eventually '[[ $(metadata_get_version "$s/metadata.mfs") -gt $shadows_version ]]'

# Verify shadow has proper metadata
metadata=$(metadata_print "${info[mount0]}")
lizardfs_master_daemon kill
lizardfs_make_conf_for_master 1
lizardfs_master_daemon reload
lizardfs_wait_for_all_ready_chunkservers
assert_no_diff "$metadata" "$(metadata_print "${info[mount0]}")"
