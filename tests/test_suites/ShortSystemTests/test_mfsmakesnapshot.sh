CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	CHUNKSERVERS=4 \
	CHUNKSERVER_LABELS="0:l0|1:l1|2:l2" \
	MASTER_CUSTOM_GOALS="10 l0l1: l0 l1|11 l1l2: l1 l2" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1|OPERATIONS_DELAY_INIT = 0:"\
	setup_local_empty_lizardfs info

# Create a directory with some files
cd "${info[mount0]}"
mkdir dir1
for i in 2 3; do
	touch dir1/file$i
	mfssetgoal $i dir1/file$i
	echo "$i$i$i" > dir1/file$i
done
assert_equals 5 $(find_all_chunks | wc -l) # First file has 2 chunks, the second one -- 3

# Test mfsmakesnapshot of the whole directory
assert_success mfsmakesnapshot dir1 dir2
expect_equals "$(ls dir1 | sort)" "$(ls dir2 | sort)"
expect_files_equal dir1/file2 dir2/file2
expect_files_equal dir1/file3 dir2/file3
expect_equals 5 $(find_all_chunks | wc -l)

# Test mfsmakesnapshot of directory content
mkdir dir3
assert_success mfsmakesnapshot dir1/ dir3
expect_equals "$(ls dir1 | sort)" "$(ls dir3 | sort)"
expect_files_equal dir1/file2 dir3/file2
expect_files_equal dir1/file3 dir3/file3
expect_equals 5 $(find_all_chunks | wc -l)

# Test mfsmakesnapshot of the whole directory into other directory
assert_success mfsmakesnapshot dir1 dir3
expect_equals "$({ ls dir1; echo dir1; } | sort)" "$(ls dir3 | sort)"
expect_equals "$(ls dir1 | sort)" "$(ls dir3/dir1 | sort)"
expect_files_equal dir1/file2 dir3/dir1/file2
expect_files_equal dir1/file3 dir3/dir1/file3
expect_equals 5 $(find_all_chunks | wc -l)

# Test mfsmakesnapshot -o on a file which should overwrite some other file
assert_success mfsmakesnapshot -o dir1/file3 dir2/file2
expect_files_equal dir1/file3 dir2/file2
expect_equals 3 "$(mfsgetgoal dir2/file2 | awk '{print $2}')"
expect_equals 5 $(find_all_chunks | wc -l)

# Test some wrong invocations
expect_failure mfsmakesnapshot dir1/file3 dir3/file2     # No -o
expect_failure mfsmakesnapshot dir1/file3 dir3/file2/    # No -o and a trailing slash
expect_failure mfsmakesnapshot -o dir1/file3 dir3/file2/ # Trailing slash prevents from this
expect_files_equal dir1/file2 dir3/file2                 # Nothing should be changed in dir3/file2!
expect_equals 5 $(find_all_chunks | wc -l)

# Snapshot of a dir, destination is a file
expect_failure mfsmakesnapshot dir1  dir3/file2
expect_failure mfsmakesnapshot dir1/ dir3/file2
expect_failure mfsmakesnapshot dir1  -o dir3/file2
expect_failure mfsmakesnapshot dir1/ -o dir3/file2
expect_files_equal dir1/file2 dir3/file2                 # Nothing should be changed in dir3/file2!
expect_equals 5 $(find_all_chunks | wc -l)

# Test multiple goals for chunks shared by snapshots
assert_success mfssetgoal -r l0l1 dir3
assert_eventually_prints '2' 'mfsfileinfo dir3/file2 | grep copy | wc -l'

assert_success mfsmakesnapshot dir3 dir4
assert_eventually_prints '2' 'mfsfileinfo dir4/file2 | grep copy | wc -l'

assert_success mfssetgoal -r l1l2 dir4
assert_eventually_prints '3' 'mfsfileinfo dir3/file2 | grep copy | wc -l'
assert_eventually_prints '3' 'mfsfileinfo dir4/file2 | grep copy | wc -l'

assert_success mfsmakesnapshot dir4 dir5
assert_eventually_prints '3' 'mfsfileinfo dir3/file2 | grep copy | wc -l'
assert_eventually_prints '3' 'mfsfileinfo dir4/file2 | grep copy | wc -l'
assert_eventually_prints '3' 'mfsfileinfo dir5/file2 | grep copy | wc -l'

assert_success mfssetgoal -r 3 dir5
assert_eventually_prints '3' 'mfsfileinfo dir3/file2 | grep copy | wc -l'
assert_eventually_prints '3' 'mfsfileinfo dir4/file2 | grep copy | wc -l'
assert_eventually_prints '3' 'mfsfileinfo dir5/file2 | grep copy | wc -l'

assert_success mfssetgoal -r 4 dir4
assert_eventually_prints '4' 'mfsfileinfo dir3/file2 | grep copy | wc -l'
assert_eventually_prints '4' 'mfsfileinfo dir4/file2 | grep copy | wc -l'
assert_eventually_prints '4' 'mfsfileinfo dir5/file2 | grep copy | wc -l'

