# Create an installation where the default goal (id=1) is "30 copies" and 20 servers
USE_RAMDISK=YES \
	CHUNKSERVERS=20 \
	MOUNT_EXTRA_CONFIG="lfscachemode=NEVER" \
	MASTER_CUSTOM_GOALS="1 default: _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
FILE_SIZE=1K file-generate file{1..25}
for file in file* ; do
	MESSAGE="Testing file $file"
	expect_success file-validate "$file"
	expect_equals 20 $(lfsfileinfo "$file" | grep copy | wc -l)
done
