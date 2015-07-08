CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	MASTER_CUSTOM_GOALS="8 X:  _ _ _ trash|14 bettergoal: _ _ _ trash trash" \
	setup_local_empty_lizardfs info

# Test set/get goal of a directory for all possible goals
cd "${info[mount0]}"
mkdir directory
for new_goal in {1..7} xor{2..9} X {9..13} bettergoal {15..20} ; do
	assert_equals "directory: $new_goal" "$(mfssetgoal "$new_goal" directory || echo FAILED)"
	assert_equals "directory: $new_goal" "$(mfsgetgoal directory || echo FAILED)"
done

# Create some files in the directory with different goals...
for goal in 2 3 5 X xor2 xor5 xor7; do
	touch directory/file$goal
	assert_success mfssetgoal $goal directory/file$goal
	assert_equals "directory/file$goal: $goal" "$(mfsgetgoal directory/file$goal)"
done


# test mfssetgoal and mfsgetgoal for multiple arguments
assert_success mfssetgoal 3 directory/file{2..3}
expect_equals $'directory/file2: 3\ndirectory/file3: 3' "$(mfsgetgoal directory/file{2..3})"

# ... and test mfssetgoal -r with different operations
assert_success mfssetgoal -r 3+ directory
expect_equals "directory/file2: 3" "$(mfsgetgoal directory/file2)"
expect_equals "directory/file3: 3" "$(mfsgetgoal directory/file3)"
expect_equals "directory/file5: 5" "$(mfsgetgoal directory/file5)"
expect_equals "directory/fileX: X" "$(mfsgetgoal directory/fileX)"
expect_equals "directory/filexor2: xor2" "$(mfsgetgoal directory/filexor2)"
expect_equals "directory/filexor5: xor5" "$(mfsgetgoal directory/filexor5)"
expect_equals "directory/filexor7: xor7" "$(mfsgetgoal directory/filexor7)"

assert_success mfssetgoal -r 4- directory
expect_equals "directory/file2: 3" "$(mfsgetgoal directory/file2)"
expect_equals "directory/file3: 3" "$(mfsgetgoal directory/file3)"
expect_equals "directory/file5: 4" "$(mfsgetgoal directory/file5)"
expect_equals "directory/fileX: 4" "$(mfsgetgoal directory/fileX)"
expect_equals "directory/filexor2: xor2" "$(mfsgetgoal directory/filexor2)"
expect_equals "directory/filexor5: xor5" "$(mfsgetgoal directory/filexor5)"
expect_equals "directory/filexor7: xor7" "$(mfsgetgoal directory/filexor7)"

assert_success mfssetgoal -r 3 directory
expect_equals "directory/file2: 3" "$(mfsgetgoal directory/file2)"
expect_equals "directory/file3: 3" "$(mfsgetgoal directory/file3)"
expect_equals "directory/file5: 3" "$(mfsgetgoal directory/file5)"
expect_equals "directory/fileX: 3" "$(mfsgetgoal directory/fileX)"
expect_equals "directory/filexor2: 3" "$(mfsgetgoal directory/filexor2)"
expect_equals "directory/filexor5: 3" "$(mfsgetgoal directory/filexor5)"
expect_equals "directory/filexor7: 3" "$(mfsgetgoal directory/filexor7)"

assert_success mfssetgoal -r bettergoal+ directory
expect_equals "directory/file2: bettergoal" "$(mfsgetgoal directory/file2)"
expect_equals "directory/file3: bettergoal" "$(mfsgetgoal directory/file3)"
expect_equals "directory/file5: bettergoal" "$(mfsgetgoal directory/file5)"
expect_equals "directory/fileX: bettergoal" "$(mfsgetgoal directory/fileX)"
expect_equals "directory/filexor2: bettergoal" "$(mfsgetgoal directory/filexor2)"
expect_equals "directory/filexor5: bettergoal" "$(mfsgetgoal directory/filexor5)"
expect_equals "directory/filexor7: bettergoal" "$(mfsgetgoal directory/filexor7)"
