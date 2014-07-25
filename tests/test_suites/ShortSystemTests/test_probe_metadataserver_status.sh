CHUNKSERVERS=1 \
	MASTERSERVERS=2 \
	USE_RAMDISK="YES" \
	MASTER_EXTRA_CONFIG="MAGIC_DISABLE_METADATA_DUMPS = 1" \
	setup_local_empty_lizardfs info

assert_eventually_prints $'master\trunning' \
		"lizardfs-probe metadataserver-status --porcelain localhost ${info[matocl]} | cut -f-2"

version=$(lizardfs-probe metadataserver-status --porcelain localhost ${info[matocl]} | cut -f3)
# Version of last changelog entry.
changelog_version=$(tail -1 "${info[master_data_path]}"/changelog.mfs | grep -o '^[0-9]*')

# Make sure probe returned correct metadata version.
assert_equals $version "$((changelog_version + 1))"

lizardfs_master_n 1 start

assert_eventually_prints $'shadow\tconnected\t'$version \
		"lizardfs-probe metadataserver-status --porcelain localhost ${info[master1_matocl]}"

lizardfs_master_n 0 stop

assert_eventually_prints $'shadow\tdisconnected\t'$version \
		"lizardfs-probe metadataserver-status --porcelain localhost ${info[master1_matocl]}"
