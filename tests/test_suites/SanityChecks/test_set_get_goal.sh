CHUNKSERVERS=1 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

expect_goal() {
	local goal=$1
	local file=$2
	local result=$(mfsgetgoal "$file")
	if [[ $result != "$file: $goal" ]]; then
		test_add_failure "Expected goal $goal, got '$result'"
	fi
}

cd "${info[mount0]}"

# Test set/get goal of a directory for all possible goals
mkdir directory
for new_goal in xor{2..9} {1..9}; do
	mfssetgoal $new_goal directory
	expect_goal $new_goal directory
done

# Create some files in the directory with different goals...
for goal in 2 3 5 xor2 xor5 xor7; do
	touch directory/file$goal
	mfssetgoal $goal directory/file$goal
	expect_goal $goal directory/file$goal
done

# ... and test mfssetgoal -r with different operations
mfssetgoal -r 3+ directory
expect_goal 3 directory/file2
expect_goal 3 directory/file3
expect_goal 5 directory/file5
expect_goal xor2 directory/filexor2
expect_goal xor5 directory/filexor5
expect_goal xor7 directory/filexor7

mfssetgoal -r 4- directory
expect_goal 3 directory/file2
expect_goal 3 directory/file3
expect_goal 4 directory/file5
expect_goal xor2 directory/filexor2
expect_goal xor5 directory/filexor5
expect_goal xor7 directory/filexor7

mfssetgoal -r xor3 directory
expect_goal xor3 directory/file2
expect_goal xor3 directory/file3
expect_goal xor3 directory/file5
expect_goal xor3 directory/filexor2
expect_goal xor3 directory/filexor5
expect_goal xor3 directory/filexor7

mfssetgoal -r 3 directory
expect_goal 3 directory/file2
expect_goal 3 directory/file3
expect_goal 3 directory/file5
expect_goal 3 directory/filexor2
expect_goal 3 directory/filexor5
expect_goal 3 directory/filexor7
