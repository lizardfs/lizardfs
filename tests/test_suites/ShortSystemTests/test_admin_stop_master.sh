USE_RAMDISK=YES \
	MASTERSERVERS=2 \
	setup_local_empty_lizardfs info

# Make sure admin stop will fail without connected shadow server
lizardfs_master_n 1 stop
assert_failure lizardfs-admin stop-master-without-saving-metadata \
		localhost "${info[matocl]}" <<< "${info[admin_password]}"
lizardfs_master_n 1 start

# Make sure admin stop will succeed with connected shadow server
assert_success lizardfs-admin stop-master-without-saving-metadata \
		localhost "${info[matocl]}" <<< "${info[admin_password]}"

# Wait for master server to actually shut down
assert_eventually "! mfsmaster -c ${info[master_cfg]} isalive"

# Make sure that there is no possibility to start the master server normally
assert_failure lizardfs_master_n 0 start

# Make sure that "-o auto-recovery" makes it possible to start the master server
assert_success lizardfs_master_n 0 start -o auto-recovery
assert_eventually "lizardfs_shadow_synchronized 1"
