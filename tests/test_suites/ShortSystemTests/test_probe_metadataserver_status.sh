CHUNKSERVERS=1 \
	MASTERSERVERS=2 \
	USE_RAMDISK="YES" \
	MASTER_EXTRA_CONFIG="MAGIC_DISABLE_METADATA_DUMPS = 1" \
	setup_local_empty_lizardfs info

assert_eventually_prints $'master\trunning\t2' \
		"lizardfs-probe metadataserver-status --porcelain localhost ${info[matocl]}"

lizardfs_master_n 1 start

assert_eventually_prints $'shadow\tconnected\t2' \
		"lizardfs-probe metadataserver-status --porcelain localhost ${info[master1_matocl]}"

lizardfs_master_n 0 stop

assert_eventually_prints $'shadow\tdisconnected\t2' \
		"lizardfs-probe metadataserver-status --porcelain localhost ${info[master1_matocl]}"
