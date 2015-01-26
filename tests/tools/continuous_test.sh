# Should be called an the beginning of each test from ContinuousTests.
# Ensures that there exists a proper environment for the test and changes directory
# to such a tests's workspace. Test is allowed to create any files there.
continuous_test_begin() {
	# This path should point to some LizardFS mount; verify it!
	assert_success mfsdirinfo -h "${LIZARDFS_MOUNTPOINT:-}"

	# Create the workspace and go there
	local workspace="$LIZARDFS_MOUNTPOINT/$TEST_SUITE_NAME/$TEST_CASE_NAME"
	assert_success mkdir -p "$workspace"
	cd "$workspace"
}
