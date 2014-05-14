USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

gid1=$(id -g lizardfstest_1)
uid1=$(id -u lizardfstest_1)
gid=$(id -g lizardfstest)
uid=$(id -u lizardfstest)

expect_failure mfssetquota -g $gid 0 0 3 6 .  # fail, permissions missing
expect_failure mfsrepquota -a .               # fail, permissions missing
expect_failure mfsrepquota -g $gid1 .         # fail, permissions missing
expect_failure mfsrepquota -u $uid1 .         # fail, permissions missing
expect_success mfsrepquota -g $gid .          # OK
expect_success mfsrepquota -u $uid .          # OK
