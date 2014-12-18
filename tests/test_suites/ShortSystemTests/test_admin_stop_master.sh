MASTER_EXTRA_CONFIG="AUTO_RECOVERY = 1" \
	AUTO_SHADOW_MASTER="NO" \
	MASTERSERVERS=2 \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

lizardfs_master_n 1 stop
# Make sure admin stop will fail without connected shadow server
assert_failure lizardfs-admin stop-master-without-saving-metadata \
		localhost "${info[matocl]}" <<< "${info[password]}"
lizardfs_master_n 1 start
# Make sure admin stop will succeed with connected shadow server
assert_success lizardfs-admin stop-master-without-saving-metadata \
		localhost "${info[matocl]}" <<< "${info[password]}"
# Wait for master server to actually shut down
assert_eventually "! mfsmaster -c ${info[master_cfg]} isalive"
lizardfs_master_n 0 start
assert_eventually "lizardfs_shadow_synchronized 1"
