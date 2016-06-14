timeout_set 45 seconds

USE_RAMDISK=YES \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota,ignoregid" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

head -c 1024 /dev/zero > file_kb
head -c $((64*1024*1024)) /dev/zero > file_chunk

one_kb_file_size=$(mfs_dir_info size file_kb)
one_chunk_file_size=$(mfs_dir_info size file_chunk)
soft=$((2*one_kb_file_size))
hard=$((3*one_kb_file_size))

mkdir dir
directory=$(readlink -m dir)

lizardfs setquota -d $soft $hard 0 0 dir
lizardfs setquota -d $soft $hard 0 0 dir

verify_dir_quota "Directory $directory -- 0 $soft $hard 0 0 0" $directory

head -c 1024 /dev/zero > dir/file_1
verify_dir_quota "Directory $directory -- $one_kb_file_size $soft $hard 1 0 0" $directory
head -c 1024 /dev/zero > dir/file_2
verify_dir_quota "Directory $directory -- $soft $soft $hard 2 0 0" $directory
head -c 1024 /dev/zero > dir/file_3
verify_dir_quota "Directory $directory +- $hard $soft $hard 3 0 0" $directory

# check if quota can't be exceeded further..
# .. by creating new files:
expect_failure head -c 1024 /dev/zero > dir/file_4
assert_equals "$(stat --format=%s dir/file_4)" 0 # file was created, but no data was written
# .. by creating new chunks for existing files:
expect_failure head -c $((64*1024*1024)) /dev/zero >> dir/file_1

# rewriting existing chunks is always possible, even after exceeding the limits:
dd if=/dev/zero of=dir/file_2 bs=1024c count=1 conv=notrunc

# truncate should always work (on files which don't have snapshots), but..
truncate -s 1P dir/file_2
dd if=/dev/zero of=dir/file_2 bs=1M seek=63 count=1 conv=notrunc
# .. one can't create new chunks:
expect_failure dd if=/dev/zero of=dir/file_2 bs=1M seek=64 count=1 conv=notrunc
truncate -s 1024 dir/file_2

# Check
verify_dir_quota "Directory $directory +- $((2 * one_kb_file_size + one_chunk_file_size)) $soft $hard 4 0 0" $directory
rm -f dir/*
verify_dir_quota "Directory $directory -- 0 $soft $hard 0 0 0" $directory

# check if snapshots are properly handled:
head -c 1024 /dev/zero > dir/file_1
lizardfs makesnapshot dir/file_1 dir/snapshot_1
verify_dir_quota "Directory $directory -- $((2 * one_kb_file_size)) $soft $hard 2 0 0" $directory

# BTW, check if '+' for soft limit is properly printed..
lizardfs setquota -d $((soft-1)) $hard 0 0 dir
verify_dir_quota "Directory $directory +- $soft $((soft-1)) $hard 2 0 0" $directory
lizardfs setquota -d $soft $hard 0 0 dir  # .. OK, come back to the previous limit

# snapshots continued..
lizardfs makesnapshot dir/file_1 dir/snapshot_2
verify_dir_quota "Directory $directory +- $hard $soft $hard 3 0 0" $directory
expect_failure lizardfs makesnapshot dir/file_1 dir/snapshot_3

# verify that we can't create new chunks by 'splitting' a chunk shared by multiple files
expect_failure dd if=/dev/zero of=dir/snapshot_2 bs=1k count=1 conv=notrunc
