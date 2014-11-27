USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="lfscachemode=NEVER" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

gid1=$(id -g lizardfstest_1)
uid1=$(id -u lizardfstest_1)
gid=$(id -g lizardfstest)
uid=$(id -u lizardfstest)

expect_failure lfssetquota -g $gid 0 0 3 6 .  # fail, permissions missing
expect_failure lfsrepquota -a .               # fail, permissions missing
expect_failure lfsrepquota -g $gid1 .         # fail, permissions missing
expect_failure lfsrepquota -u $uid1 .         # fail, permissions missing
expect_success lfsrepquota -g $gid .          # OK
expect_success lfsrepquota -u $uid .          # OK
