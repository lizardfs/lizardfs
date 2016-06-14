# verify_quota <expected limits> <group name>
# e.g. verify_quota "Group $gid1 -- $size $hard $soft $inodes $hard_ino $soft_ino" lizardfstest_1
function verify_quota {
	local expected_limits=$1
	local group=$2
	local gid=$(id -g $group)
	assert_equals "$expected_limits" \
		"$(lizardfs repquota -g $gid . | trim_hard | grep "Group $gid")" > /dev/null
}

function verify_dir_quota {
	local expected_limits=$1
	local directory=$2
	assert_equals "$expected_limits" \
		"$(lizardfs repquota -d $directory | trim_hard | grep "Directory $directory")" > /dev/null
}

