USE_RAMDISK=YES \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota,ignoregid" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

gid1=$(id -g lizardfstest_1)
gid=$(id -g lizardfstest)

sudo -nu lizardfstest_3 bash -c 'head -c 1024 /dev/zero > file_kb'
sudo -nu lizardfstest_3 bash -c 'head -c $((64*1024*1024)) /dev/zero > file_chunk'

one_kb_file_size=$(mfs_dir_info size file_kb)
one_chunk_file_size=$(mfs_dir_info size file_chunk)
soft=$((2*one_kb_file_size))
hard=$((3*one_kb_file_size))

mfssetquota -g $gid1 $soft $hard 0 0 .
mfssetquota -g $gid $soft $hard 0 0 .

verify_quota "Group $gid1 -- 0 $soft $hard 0 0 0" lizardfstest_1
verify_quota "Group $gid -- 0 $soft $hard 0 0 0" lizardfstest

sudo -nu lizardfstest_1 bash -c 'head -c 1024 /dev/zero > file_1'
verify_quota "Group $gid1 -- $one_kb_file_size $soft $hard 1 0 0" lizardfstest_1
sudo -nu lizardfstest_1 bash -c 'head -c 1024 /dev/zero > file_2'
verify_quota "Group $gid1 -- $soft $soft $hard 2 0 0" lizardfstest_1
sudo -nu lizardfstest_1 bash -c 'head -c 1024 /dev/zero > file_3'
verify_quota "Group $gid1 +- $hard $soft $hard 3 0 0" lizardfstest_1

# check if quota can't be exceeded further..
# .. by creating new files:
expect_failure sudo -nu lizardfstest_1 bash -c 'head -c 1024 /dev/zero > file_4'
assert_equals "$(stat --format=%s file_4)" 0 # file was created, but no data was written
# .. by creating new chunks for existing files:
expect_failure sudo -nu lizardfstest_1 bash -c 'head -c $((64*1024*1024)) /dev/zero >> file_1'

# rewriting existing chunks is always possible, even after exceeding the limits:
sudo -nu lizardfstest_1 dd if=/dev/zero of=file_2 bs=1024c count=1 conv=notrunc

# truncate should always work (on files which don't have snapshots), but..
sudo -nu lizardfstest_1 truncate -s 1P file_2
sudo -nu lizardfstest_1 dd if=/dev/zero of=file_2 bs=1M seek=63 count=1 conv=notrunc
# .. one can't create new chunks:
expect_failure sudo -nu lizardfstest_1 dd if=/dev/zero of=file_2 bs=1M seek=64 count=1 conv=notrunc
sudo -nu lizardfstest_1 truncate -s 1024 file_2

# changing group should affect usage:
sudo -nu lizardfstest_1 chgrp $gid file_2
verify_quota "Group $gid1 +- $((one_kb_file_size + one_chunk_file_size)) $soft $hard 3 0 0" \
		lizardfstest_1
verify_quota "Group $gid -- $one_kb_file_size $soft $hard 1 0 0" lizardfstest

# check if snapshots are properly handled:
mfsmakesnapshot file_2 snapshot_1
verify_quota "Group $gid -- $soft $soft $hard 2 0 0" lizardfstest

# BTW, check if '+' for soft limit is properly printed..
mfssetquota -g $gid $((soft-1)) $hard 0 0 .
verify_quota "Group $gid +- $soft $((soft-1)) $hard 2 0 0" lizardfstest
mfssetquota -g $gid $soft $hard 0 0 .  # .. OK, come back to the previous limit

# snapshots continued..
mfsmakesnapshot file_2 snapshot_2
verify_quota "Group $gid +- $hard $soft $hard 3 0 0" lizardfstest
expect_failure mfsmakesnapshot file_2 snapshot_3

# verify that we can't create new chunks by 'splitting' a chunk shared by multiple files
expect_failure sudo -nu lizardfstest_1 dd if=/dev/zero of=snapshot_2 bs=1k count=1 conv=notrunc

# hard links don't occupy any new space, therefore are always permitted
sudo -nu lizardfstest_1 ln file_2 hard_link
verify_quota "Group $gid1 +- $((one_kb_file_size + one_chunk_file_size)) $soft $hard 3 0 0" \
		lizardfstest_1

#It would be nice to test chown as well, but I don't know how to do that without using superuser

