assert_program_installed pylint

# Get paths to mfs.cgi, chart.cgi and the CGI server
files=$(echo $LIZARDFS_ROOT/share/mfscgi/*.cgi $LIZARDFS_ROOT/sbin/lizardfs-cgiserver)

# Validate all found files using pylint
expect_empty "$(pylint -E $files || true)"
