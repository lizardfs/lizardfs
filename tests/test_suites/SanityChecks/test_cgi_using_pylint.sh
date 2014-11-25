assert_program_installed pylint

# Validate mfs.cgi and chart.cgi files using pylint
expect_empty "$(pylint -E $LIZARDFS_ROOT/share/mfscgi/*.cgi || true)"
