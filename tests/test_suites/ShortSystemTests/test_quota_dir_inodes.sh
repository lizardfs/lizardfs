USE_RAMDISK=YES \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota,ignoregid" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

softlimit=3
hardlimit=14

mkdir dir
directory=$(readlink -m dir)

mfssetquota -d 0 0 $softlimit $hardlimit dir

# exceed quota by creating 1 directory and some files (8 inodes in total):
for i in {1..4}; do
	verify_dir_quota "Directory $directory -- 0 0 0 $((i-1)) $softlimit $hardlimit" $directory
	touch dir/file$i
done
for i in 5 6; do
	# after exceeding soft limit - changed into +:
	verify_dir_quota "Directory $directory -+ 0 0 0 $((i-1)) $softlimit $hardlimit" $directory
	touch dir/file$i
done

# soft links do affect usage and are checked against limits:
ln -s dir/file4 dir/soft1
verify_dir_quota "Directory $directory -+ 0 0 0 7 $softlimit $hardlimit" $directory

# snapshots are allowed, if none of the uid/gid of files residing
# in a directory reached its limit:
for i in {1..6}; do
	mfsmakesnapshot dir/file$i dir/snapshot_file$i
done
mfsmakesnapshot dir/soft1 dir/snapshot_soft1

verify_dir_quota "Directory $directory -+ 0 0 0 14 $softlimit $hardlimit" $directory

# check if quota can't be exceeded further:
expect_failure touch dir/file
expect_failure mkdir dir/dir
expect_failure ln -s dir/file4 dir/soft2
expect_failure mfsmakesnapshot dir/file2 dir/snapshot2
verify_dir_quota "Directory $directory -+ 0 0 0 14 $softlimit $hardlimit" $directory

# hard links don't affect usage and are not checked against limits:
ln dir/file4 hard1
verify_dir_quota "Directory $directory -+ 0 0 0 14 $softlimit $hardlimit" $directory

# check if removing directory removes quota
rm -Rf dir
assert_equals "$(mfsrepquota -a . | grep ^Directory | cat)" ""
mkdir dir
assert_equals "$(mfsrepquota -a . | grep ^Directory | cat)" ""

# verify nested quotas
mkdir dir/dir1
mkdir dir/dir2
mkdir dir/dir3

softlimit=5
hardlimit=5
parent_softlimit=15
parent_hardlimit=15

mfssetquota -d 0 0 $parent_softlimit $parent_hardlimit dir
mfssetquota -d 0 0 $softlimit $hardlimit dir/dir1
mfssetquota -d 0 0 $softlimit $hardlimit dir/dir2
mfssetquota -d 0 0 $softlimit $hardlimit dir/dir3

directory1=$(readlink -m dir/dir1)
directory2=$(readlink -m dir/dir2)
directory3=$(readlink -m dir/dir3)

# create files in first subdirectory directory
for i in {1..5}; do
	verify_dir_quota "Directory $directory1 -- 0 0 0 $((i-1)) $softlimit $hardlimit" $directory1
	touch dir/dir1/file$i
done
expect_failure touch dir/dir1/file
verify_dir_quota "Directory $directory -- 0 0 0 8 $parent_softlimit $parent_hardlimit" $directory

# create files in second subdirectory
for i in {1..5}; do
	verify_dir_quota "Directory $directory2 -- 0 0 0 $((i-1)) $softlimit $hardlimit" $directory2
	touch dir/dir2/file$i
done
expect_failure touch dir/dir2/file
verify_dir_quota "Directory $directory -- 0 0 0 13 $parent_softlimit $parent_hardlimit" $directory

# create files in third subdirectory
for i in {1..2}; do
	verify_dir_quota "Directory $directory3 -- 0 0 0 $((i-1)) $softlimit $hardlimit" $directory3
	touch dir/dir3/file$i
done
# creating file should fail because of quota on parent directory
expect_failure touch dir/dir3/file3

verify_dir_quota "Directory $directory -+ 0 0 0 15 $parent_softlimit $parent_hardlimit" $directory
