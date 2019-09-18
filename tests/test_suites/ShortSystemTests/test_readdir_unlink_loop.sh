timeout_set 5 minutes

setup_local_empty_lizardfs info
cd ${info[mount0]}
assert_success readdir-unlink-test 1024
