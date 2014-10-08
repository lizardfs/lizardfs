cat > "$TEMP_DIR/goalconf" <<END
 8 X:  _ _ _ trash
14 bettergoal: _ _ _ trash trash
END

CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	MASTER_EXTRA_CONFIG="CUSTOM_GOALS_FILENAME=$TEMP_DIR/goalconf"
	setup_local_empty_lizardfs info

# Test set/get goal of a directory for all possible goals
cd "${info[mount0]}"
mkdir directory
for new_goal in {1..7} X {9..13} bettergoal {15..20} ; do
	assert_equals "directory: $new_goal" "$(mfssetgoal "$new_goal" directory || echo FAILED)"
	assert_equals "directory: $new_goal" "$(mfsgetgoal directory || echo FAILED)"
done

# Create some files in the directory with different goals...
for goal in 2 3 5 X; do
	touch directory/file$goal
	assert_success mfssetgoal $goal directory/file$goal
	assert_equals "directory/file$goal: $goal" "$(mfsgetgoal directory/file$goal)"
done

# ... and test mfssetgoal -r with different operations
assert_success mfssetgoal -r 3+ directory
expect_equals "directory/file2: 3" "$(mfsgetgoal directory/file2)"
expect_equals "directory/file3: 3" "$(mfsgetgoal directory/file3)"
expect_equals "directory/file5: 5" "$(mfsgetgoal directory/file5)"
expect_equals "directory/fileX: X" "$(mfsgetgoal directory/fileX)"

assert_success mfssetgoal -r 4- directory
expect_equals "directory/file2: 3" "$(mfsgetgoal directory/file2)"
expect_equals "directory/file3: 3" "$(mfsgetgoal directory/file3)"
expect_equals "directory/file5: 4" "$(mfsgetgoal directory/file5)"
expect_equals "directory/fileX: 4" "$(mfsgetgoal directory/fileX)"

assert_success mfssetgoal -r 3 directory
expect_equals "directory/file2: 3" "$(mfsgetgoal directory/file2)"
expect_equals "directory/file3: 3" "$(mfsgetgoal directory/file3)"
expect_equals "directory/file5: 3" "$(mfsgetgoal directory/file5)"
expect_equals "directory/fileX: 3" "$(mfsgetgoal directory/fileX)"

assert_success mfssetgoal -r bettergoal+ directory
expect_equals "directory/file2: bettergoal" "$(mfsgetgoal directory/file2)"
expect_equals "directory/file3: bettergoal" "$(mfsgetgoal directory/file3)"
expect_equals "directory/file5: bettergoal" "$(mfsgetgoal directory/file5)"
expect_equals "directory/fileX: bettergoal" "$(mfsgetgoal directory/fileX)"
