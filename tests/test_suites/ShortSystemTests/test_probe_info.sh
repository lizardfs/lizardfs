CHUNKSERVERS=4 \
	MASTER_EXTRA_CONFIG="REPLICATIONS_DELAY_INIT = 100000" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
goals="2 3 4"
for goal in $goals; do
	mkdir dir_$goal
	mfssetgoal $goal dir_$goal
	FILE_SIZE=150K file-generate dir_$goal/file
done

rm dir_3/file
expect_equals "$LIZARDFS_VERSION 1 0 0 7 4 3 3 9 9" \
	"$(lizardfs-probe info --porcelain localhost "${info[matocl]}" | cut -d' ' -f 1,6-)"
