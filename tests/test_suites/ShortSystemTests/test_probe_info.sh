CHUNKSERVERS=4 \
	MASTER_EXTRA_CONFIG="OPERATIONS_DELAY_INIT = 100000" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
goals="2 3 xor2 xor3"
for goal in $goals; do
	mkdir dir_$goal
	lizardfs setgoal $goal dir_$goal
	FILE_SIZE=150K file-generate dir_$goal/file
done

rm dir_3/file
rm dir_xor2/file
expect_equals "$LIZARDFS_VERSION 2 0 0 9 5 4 4 7 7" \
	"$(lizardfs-probe info --porcelain localhost "${info[matocl]}" | cut -d' ' -f 1,6-)"
