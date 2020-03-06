timeout_set 1 minutes

master_cfg="MAGIC_DISABLE_METADATA_DUMPS = 1"
master_cfg+="|AUTO_RECOVERY = 1"

CHUNKSERVERS=1 \
	MASTERSERVERS=2 \
	MOUNTS=2 \
	USE_RAMDISK="YES" \
	MOUNT_0_EXTRA_CONFIG="mfscachemode=NEVER,mfsreportreservedperiod=1" \
	MOUNT_1_EXTRA_CONFIG="mfsmeta" \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota, ignoregid" \
	MFSEXPORTS_META_EXTRA_OPTIONS="nonrootmeta" \
	MASTER_EXTRA_CONFIG="$master_cfg" \
	setup_local_empty_lizardfs info

stat_basic_info() {
	stat --format="%A %u %g %i %s" "$@"
}

only_file_in_trash() {
	assert_equals "1" "$(ls "$trash" | grep -v undel | wc -l)"
	echo "$trash/$(ls "$trash" | grep -v undel)"
}

trash="${info[mount1]}/trash"
changelog_file="${info[master_data_path]}"/changelog.mfs

lizardfs_master_n 1 start
assert_eventually "lizardfs_shadow_synchronized 1"

# Generate file for removal
cd "${info[mount0]}"
mkdir -p a/b/c
FILE_SIZE=1M file-generate a/b/c/test

stat_before_rm=$(stat_basic_info a/b/c/test)

# remove all files and directories
rm -rf a

# undelete file "a/b/c/test"
file_in_trash=$(only_file_in_trash)
assert_success mv "$file_in_trash" "$trash/undel/"

# check if path was recreated
assert_success file-validate a/b/c/test
stat_after_recovery=$(stat_basic_info a/b/c/test)
assert_equals "$stat_before_rm" "$stat_after_recovery"

# check if creating files was issued before undelete in changelog
create_count=$(cat $changelog_file | grep -B 3 UNDEL | grep CREATE | wc -l)
assert_equals "$create_count" "3"

assert_eventually "lizardfs_shadow_synchronized 1"
