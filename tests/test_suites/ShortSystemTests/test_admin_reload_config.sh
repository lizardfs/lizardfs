reload_log="${TEMP_DIR}/reloads"

password="good-password"
USE_RAMDISK="YES" \
	ADMIN_PASSWORD="$password" \
	MASTER_EXTRA_CONFIG="MAGIC_DEBUG_LOG=${reload_log}|LOG_FLUSH_ON=DEBUG" \
	setup_local_empty_lizardfs info

master_port="${lizardfs_info_[matocl]}"
shadow_port="${lizardfs_info_[masterauto_matocl]}"

# There ahould be no reloads after starting the system
assert_equals 0 $(grep main.reload "$reload_log" | wc -l)

# Make sure that wrong password doesn't work
assert_failure lizardfs-admin reload-config localhost "$master_port" <<< "wrong-password"
assert_equals 0 $(grep main.reload "$reload_log" | wc -l)

# Make sure that good password works
assert_success lizardfs-admin reload-config localhost "$master_port" <<< "$password"
assert_eventually_prints 1 'grep main.reload "$reload_log" | wc -l'

# Make sure that the usual reload mechanism also works
lizardfs_master_daemon reload
assert_eventually_prints 2 'grep main.reload "$reload_log" | wc -l'

# Make sure that reload-config still works
assert_success lizardfs-admin reload-config localhost "$master_port" <<< "$password"
assert_eventually_prints 3 'grep main.reload "$reload_log" | wc -l'

# Make sure that shadow masters also accept this command
assert_success lizardfs-admin reload-config localhost "$shadow_port" <<< "$password"
assert_eventually_prints 4 'grep main.reload "$reload_log" | wc -l'
