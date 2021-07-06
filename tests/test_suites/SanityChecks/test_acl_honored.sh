assert_program_installed setfacl getfacl

MOUNT_EXTRA_CONFIG="mfsacl" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

uname --all

test_dir_permissions=${info[mount0]}/dir

sudo su --login root --command "mkdir --mode 000 ${test_dir_permissions}"
assert_failure sudo su --login lizardfstest_1 --command "ls -l --all ${test_dir_permissions}"

setfacl --modify u:lizardfstest_1:rwx ${test_dir_permissions}
assert_success sudo su --login lizardfstest_1 --command "ls -l --all ${test_dir_permissions}"

setfacl --remove-all ${test_dir_permissions}
assert_failure sudo su --login lizardfstest_1 --command "ls -l --all ${test_dir_permissions}"
