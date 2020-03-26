# TODO Fix this test

test_end

# Create an installation where the default goal (id=1) is "30 copies" and 20 servers
USE_RAMDISK=YES \
	CHUNKSERVERS=3 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_CUSTOM_GOALS="1 default: _ _|2 tapegoal: _ _ _@" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
FILE_SIZE=1K file-generate file{1..25}

assert_success lizardfs setgoal -r tapegoal .

# Make sure files are readable and have proper goal
for file in file* ; do
	MESSAGE="Testing file $file"
	expect_success file-validate "$file"
	expect_equals 2 $(lizardfs fileinfo "$file" | grep copy | wc -l)
done

# Make sure files are read-only
for file in file* ; do
	# File permissions can be changed
	assert_success chmod +r $file
	# but it should not affect it being read-only
	assert_failure dd if=/dev/zero of=$file bs=1024 count=4
done
