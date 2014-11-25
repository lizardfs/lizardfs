CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="lfscachemode=NEVER" \
	setup_local_empty_lizardfs info

# Create a directory with some files
cd "${info[mount0]}"
mkdir dir1
for i in 2 3; do
	touch dir1/file$i
	lfssetgoal $i dir1/file$i
	echo "$i$i$i" > dir1/file$i
done
assert_equals 5 $(find_all_chunks | wc -l) # First file has 2 chunks, the second one -- 3

# Test lfsmakesnapshot of the whole directory
assert_success lfsmakesnapshot dir1 dir2
expect_equals "$(ls dir1 | sort)" "$(ls dir2 | sort)"
expect_files_equal dir1/file2 dir2/file2
expect_files_equal dir1/file3 dir2/file3
expect_equals 5 $(find_all_chunks | wc -l)

# Test lfsmakesnapshot of directory content
mkdir dir3
assert_success lfsmakesnapshot dir1/ dir3
expect_equals "$(ls dir1 | sort)" "$(ls dir3 | sort)"
expect_files_equal dir1/file2 dir3/file2
expect_files_equal dir1/file3 dir3/file3
expect_equals 5 $(find_all_chunks | wc -l)

# Test lfsmakesnapshot of the whole directory into other directory
assert_success lfsmakesnapshot dir1 dir3
expect_equals "$({ ls dir1; echo dir1; } | sort)" "$(ls dir3 | sort)"
expect_equals "$(ls dir1 | sort)" "$(ls dir3/dir1 | sort)"
expect_files_equal dir1/file2 dir3/dir1/file2
expect_files_equal dir1/file3 dir3/dir1/file3
expect_equals 5 $(find_all_chunks | wc -l)

# Test lfsmakesnapshot -o on a file which should overwrite some other file
assert_success lfsmakesnapshot -o dir1/file3 dir2/file2
expect_files_equal dir1/file3 dir2/file2
expect_equals 3 "$(lfsgetgoal dir2/file2 | awk '{print $2}')"
expect_equals 5 $(find_all_chunks | wc -l)

# Test some wrong invocations
expect_failure lfsmakesnapshot dir1/file3 dir3/file2     # No -o
expect_failure lfsmakesnapshot dir1/file3 dir3/file2/    # No -o and a trailing slash
expect_failure lfsmakesnapshot -o dir1/file3 dir3/file2/ # Trailing slash prevents from this
expect_files_equal dir1/file2 dir3/file2                 # Nothing should be changed in dir3/file2!
expect_equals 5 $(find_all_chunks | wc -l)

# Snapshot of a dir, destination is a file
expect_failure lfsmakesnapshot dir1  dir3/file2
expect_failure lfsmakesnapshot dir1/ dir3/file2
expect_failure lfsmakesnapshot dir1  -o dir3/file2
expect_failure lfsmakesnapshot dir1/ -o dir3/file2
expect_files_equal dir1/file2 dir3/file2                 # Nothing should be changed in dir3/file2!
expect_equals 5 $(find_all_chunks | wc -l)
