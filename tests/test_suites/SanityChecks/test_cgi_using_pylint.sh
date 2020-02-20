if is_program_installed pylint && pylint --version | grep -q "Python 3" ; then
	pylintexec="pylint"
elif is_program_installed pylint3 ; then
	pylintexec="pylint3"
else
	test_fail "pylint is not installed"
fi

# Get paths to mfs.cgi, chart.cgi and the CGI server
files=$(echo $LIZARDFS_ROOT/share/mfscgi/*.cgi $LIZARDFS_ROOT/sbin/lizardfs-cgiserver)

# Validate all found files using pylint
expect_empty "$($pylintexec -E $files || true)"
