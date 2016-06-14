USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

gid1=$(id -g lizardfstest_1)
uid1=$(id -u lizardfstest_1)
gid=$(id -g lizardfstest)
uid=$(id -u lizardfstest)

expect_failure lizardfs setquota -g $gid 0 0 3 6 .  # fail, permissions missing
expect_failure lizardfs repquota -a .               # fail, permissions missing
expect_failure lizardfs repquota -g $gid1 .         # fail, permissions missing
expect_failure lizardfs repquota -u $uid1 .         # fail, permissions missing
expect_success lizardfs repquota -g $gid .          # OK
expect_success lizardfs repquota -u $uid .          # OK
