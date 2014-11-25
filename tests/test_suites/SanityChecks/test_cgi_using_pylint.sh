assert_program_installed pylint

# Validate lfs.cgi and chart.cgi files using pylint
expect_empty "$(pylint -E $LIZARDFS_ROOT/share/lfscgi/*.cgi || true)"
