CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	MASTER_CUSTOM_GOALS="8 X:  _ _ _ trash|14 bettergoal: _ _ _ trash trash" \
	setup_local_empty_lizardfs info

# Test set/get goal of a directory for all possible goals
cd "${info[mount0]}"
mkdir directory
for new_goal in {1..7} xor{2..9} X {9..13} bettergoal {15..20} ; do
	assert_equals "directory: $new_goal" "$(lizardfs setgoal "$new_goal" directory || echo FAILED)"
	assert_equals "directory: $new_goal" "$(lizardfs getgoal directory || echo FAILED)"
done

# Create some files in the directory with different goals...
for goal in 2 3 5 X xor2 xor5 xor7; do
	touch directory/file$goal
	assert_success lizardfs setgoal $goal directory/file$goal
	assert_equals "directory/file$goal: $goal" "$(lizardfs getgoal directory/file$goal)"
done


# test lizardfs setgoal and lizardfs getgoal for multiple arguments
assert_success lizardfs setgoal 3 directory/file{2..3}
expect_equals $'directory/file2: 3\ndirectory/file3: 3' "$(lizardfs getgoal directory/file{2..3})"

# ... and test lizardfs setgoal -r with different operations
assert_success lizardfs setgoal -r 3 directory
expect_equals "directory/file2: 3" "$(lizardfs getgoal directory/file2)"
expect_equals "directory/file3: 3" "$(lizardfs getgoal directory/file3)"
expect_equals "directory/file5: 3" "$(lizardfs getgoal directory/file5)"
expect_equals "directory/fileX: 3" "$(lizardfs getgoal directory/fileX)"
expect_equals "directory/filexor2: 3" "$(lizardfs getgoal directory/filexor2)"
expect_equals "directory/filexor5: 3" "$(lizardfs getgoal directory/filexor5)"
expect_equals "directory/filexor7: 3" "$(lizardfs getgoal directory/filexor7)"

assert_success lizardfs setgoal -r 4 directory
expect_equals "directory/file2: 4" "$(lizardfs getgoal directory/file2)"
expect_equals "directory/file3: 4" "$(lizardfs getgoal directory/file3)"
expect_equals "directory/file5: 4" "$(lizardfs getgoal directory/file5)"
expect_equals "directory/fileX: 4" "$(lizardfs getgoal directory/fileX)"
expect_equals "directory/filexor2: 4" "$(lizardfs getgoal directory/filexor2)"
expect_equals "directory/filexor5: 4" "$(lizardfs getgoal directory/filexor5)"
expect_equals "directory/filexor7: 4" "$(lizardfs getgoal directory/filexor7)"

assert_success lizardfs setgoal -r 3 directory
expect_equals "directory/file2: 3" "$(lizardfs getgoal directory/file2)"
expect_equals "directory/file3: 3" "$(lizardfs getgoal directory/file3)"
expect_equals "directory/file5: 3" "$(lizardfs getgoal directory/file5)"
expect_equals "directory/fileX: 3" "$(lizardfs getgoal directory/fileX)"
expect_equals "directory/filexor2: 3" "$(lizardfs getgoal directory/filexor2)"
expect_equals "directory/filexor5: 3" "$(lizardfs getgoal directory/filexor5)"
expect_equals "directory/filexor7: 3" "$(lizardfs getgoal directory/filexor7)"

assert_success lizardfs setgoal -r bettergoal directory
expect_equals "directory/file2: bettergoal" "$(lizardfs getgoal directory/file2)"
expect_equals "directory/file3: bettergoal" "$(lizardfs getgoal directory/file3)"
expect_equals "directory/file5: bettergoal" "$(lizardfs getgoal directory/file5)"
expect_equals "directory/fileX: bettergoal" "$(lizardfs getgoal directory/fileX)"
expect_equals "directory/filexor2: bettergoal" "$(lizardfs getgoal directory/filexor2)"
expect_equals "directory/filexor5: bettergoal" "$(lizardfs getgoal directory/filexor5)"
expect_equals "directory/filexor7: bettergoal" "$(lizardfs getgoal directory/filexor7)"
