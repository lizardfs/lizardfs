# Mount1 will be used for meta operations
MOUNTS=2 \
	CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	MASTER_EXTRA_CONFIG="EMPTY_TRASH_PERIOD = 1" \
	MFSEXPORTS_META_EXTRA_OPTIONS="nonrootmeta" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MOUNT_1_EXTRA_CONFIG="mfsmeta" \
	setup_local_empty_lizardfs info

stat_basic_info() {
	stat --format="%A %u %g %i %s" "$@"
}

trash="${info[mount1]}/trash"

only_file_in_trash() {
	assert_equals "1" "$(ls "$trash" | grep -v undel | wc -l)"
	echo "$trash/$(ls "$trash" | grep -v undel)"
}

# 1. Prepare environment
cd "${info[mount0]}"
echo abcd > source
ln -s source symlink
ln source link

mkdir dir dir2
mfssetgoal 1 dir
mfssettrashtime 10000 dir dir2
cd dir
FILE_SIZE=1M file-generate file file2

# 2. Recover file from trash
stat_before_rm=$(stat_basic_info file)
rm file
assert_failure stat file
assert_failure file-validate file
file_in_trash=$(only_file_in_trash)
assert_success stat "$file_in_trash" >/dev/null
assert_success mv "$file_in_trash" "$trash/undel/"

assert_success file-validate file
stat_after_recovery=$(stat_basic_info file)
assert_equals "$stat_before_rm" "$stat_after_recovery"

# 3. Empty trash after trashtime
trash_time=11
mfssettrashtime $trash_time file
begin_ts=$(timestamp)
rm file
assert_success stat "$file_in_trash" >/dev/null
assert_success wait_for '[ $(ls "$trash" | grep -v undel | wc -l) == 0 ] ' "$((trash_time + 10)) seconds"
end_ts=$(timestamp)
duration=$((end_ts - begin_ts))
assert_less_or_equal trash_time $trash_time $duration
assert_near $trash_time $duration 3

# 4. Remove nodes other than ordinary files
cd ..
stat_before_rm=$(stat_basic_info dir/file2)
rm dir -r
assert_equals 1 $(ls "$trash" | grep -v undel | wc -l) # Only file2 is in trash
rm symlink link
assert_equals 1 $(ls "$trash" | grep -v undel | wc -l) # Links and directories don't get to trash

# 5. Recover file to a different location
echo a/b/c > "$(only_file_in_trash)"
mv "$(only_file_in_trash)" "$trash/undel"
assert_success file-validate a/b/c
stat_after_recovery=$(stat_basic_info a/b/c)
assert_equals "$stat_before_rm" "$stat_after_recovery"
