USE_RAMDISK=YES \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota,ignoregid" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

gid1=$(id -g lizardfstest_1)
gid2=$(id -g lizardfstest)

mfssetquota -g $gid1 0 0 3 8 .

# exceed quota by creating 1 directory and some files (8 inodes in total):
sudo -nu lizardfstest_1 mkdir dir_$gid1
for i in 2 3 4; do
	verify_quota "Group $gid1 -- 0 0 0 $((i-1)) 3 8" lizardfstest_1
	sudo -nu lizardfstest_1 touch dir_$gid1/$i
done
for i in 5 6; do
	# after exceeding soft limit - changed into +:
	verify_quota "Group $gid1 -+ 0 0 0 $((i-1)) 3 8" lizardfstest_1
	sudo -nu lizardfstest_1 touch dir_$gid1/$i
done

# soft links do affect usage and are checked against limits:
sudo -nu lizardfstest_1 ln -s dir_$gid1/4 dir_$gid1/soft1
verify_quota "Group $gid1 -+ 0 0 0 7 3 8" lizardfstest_1

# snapshots are allowed, if none of the uid/gid of files residing
# in a directory reached its limit:
sudo -nu lizardfstest_1 $(which mfsmakesnapshot) dir_$gid1 snapshot
# sudo does not necessarily pass '$PATH', even if -E is used, that's
# why a workaround with 'which' was used above
verify_quota "Group $gid1 -+ 0 0 0 14 3 8" lizardfstest_1

# check if quota can't be exceeded further:
expect_failure sudo -nu lizardfstest_1 touch dir_$gid1/file
expect_failure sudo -nu lizardfstest_1 mkdir dir2_$gid1
expect_failure sudo -nu lizardfstest_1 ln -s dir_$gid1/4 dir_$gid1/soft2
expect_failure sudo -nu lizardfstest_1 $(which mfsmakesnapshot) dir_$gid1 snapshot2
verify_quota "Group $gid1 -+ 0 0 0 14 3 8" lizardfstest_1

# hard links don't affect usage and are not checked against limits:
sudo -nu lizardfstest_1 ln dir_$gid1/4 hard
verify_quota "Group $gid1 -+ 0 0 0 14 3 8" lizardfstest_1

# check if chgrp is properly handled
sudo -nu lizardfstest_1 chgrp -R $gid2 dir_$gid1
verify_quota "Group $gid1 -+ 0 0 0 7 3 8" lizardfstest_1
verify_quota "Group $gid2 -- 0 0 0 7 0 0" lizardfstest

# check if quota can't be exceeded by one:
sudo -nu lizardfstest_1 touch dir_$gid1/file1
expect_failure sudo -nu lizardfstest_1 touch dir_$gid1/file2
verify_quota "Group $gid1 -+ 0 0 0 8 3 8" lizardfstest_1

#It would be nice to test chown as well, but I don't know how to do that without using superuser

