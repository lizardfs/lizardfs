CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	MASTER_CUSTOM_GOALS="8 X:  _ _ _ trash|14 bettergoal: _ _ _ trash trash" \
	setup_local_empty_lizardfs info

# Test set/get goal of a directory for all possible goals
cd "${info[mount0]}"
mkdir directory
for new_goal in {1..7} X {9..13} bettergoal {15..20} ; do
	assert_equals "directory: $new_goal" "$(lfssetgoal "$new_goal" directory || echo FAILED)"
	assert_equals "directory: $new_goal" "$(lfsgetgoal directory || echo FAILED)"
done

# Create some files in the directory with different goals...
for goal in 2 3 5 X; do
	touch directory/file$goal
	assert_success lfssetgoal $goal directory/file$goal
	assert_equals "directory/file$goal: $goal" "$(lfsgetgoal directory/file$goal)"
done


# test lfssetgoal and lfsgetgoal for multiple arguments
assert_success lfssetgoal 3 directory/file{2..3}
expect_equals $'directory/file2: 3\ndirectory/file3: 3' "$(lfsgetgoal directory/file{2..3})"

# ... and test lfssetgoal -r with different operations
assert_success lfssetgoal -r 3+ directory
expect_equals "directory/file2: 3" "$(lfsgetgoal directory/file2)"
expect_equals "directory/file3: 3" "$(lfsgetgoal directory/file3)"
expect_equals "directory/file5: 5" "$(lfsgetgoal directory/file5)"
expect_equals "directory/fileX: X" "$(lfsgetgoal directory/fileX)"

assert_success lfssetgoal -r 4- directory
expect_equals "directory/file2: 3" "$(lfsgetgoal directory/file2)"
expect_equals "directory/file3: 3" "$(lfsgetgoal directory/file3)"
expect_equals "directory/file5: 4" "$(lfsgetgoal directory/file5)"
expect_equals "directory/fileX: 4" "$(lfsgetgoal directory/fileX)"

assert_success lfssetgoal -r 3 directory
expect_equals "directory/file2: 3" "$(lfsgetgoal directory/file2)"
expect_equals "directory/file3: 3" "$(lfsgetgoal directory/file3)"
expect_equals "directory/file5: 3" "$(lfsgetgoal directory/file5)"
expect_equals "directory/fileX: 3" "$(lfsgetgoal directory/fileX)"

assert_success lfssetgoal -r bettergoal+ directory
expect_equals "directory/file2: bettergoal" "$(lfsgetgoal directory/file2)"
expect_equals "directory/file3: bettergoal" "$(lfsgetgoal directory/file3)"
expect_equals "directory/file5: bettergoal" "$(lfsgetgoal directory/file5)"
expect_equals "directory/fileX: bettergoal" "$(lfsgetgoal directory/fileX)"
