MOUNTS=2 \
	CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MFSEXPORTS_META_EXTRA_OPTIONS="nonrootmeta" \
	setup_local_empty_lizardfs info

# In this test mountpoint #0 will be used for ordinary operations
# and mountpoint #1 will be used for meta operations

fusermount -u "${info[mount1]}"
FUSE_EXTRA_CONFIG="mfsmeta" add_mount 1

stat_basic_info() {
	stat --format="%A %u %g %i %s" "$@"
}

cd "${info[mount0]}"
mkdir dir
mfssetgoal 1 dir
mfssettrashtime 10000 dir
cd dir

FILE_SIZE=1M file-generate file
stat_before_rm=$(stat_basic_info file)

rm file
assert_failure stat file
assert_failure file-validate file

assert_success stat "${info[mount1]}/trash/00000003|dir|file" >/dev/null
assert_success mv "${info[mount1]}/trash/00000003|dir|file" "${info[mount1]}/trash/undel/"

assert_success file-validate file
stat_after_recovery=$(stat_basic_info file)
assert_equals "$stat_before_rm" "$stat_after_recovery"

