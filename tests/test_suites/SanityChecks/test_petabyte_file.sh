CHUNKSERVERS=2 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="lfscachemode=NEVER" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
truncate -s 1P petabyte_sparse_file
assert_equals $(parse_si_suffix 1P) $(stat --format="%s" petabyte_sparse_file)
expect_equals 0 $(lfs_dir_info realsize petabyte_sparse_file)
if ! cmp <(head -c 10000 /dev/zero) <(head -c 10000 petabyte_sparse_file); then
	test_add_failure "Sparse file contains non-zero bytes"
fi

echo -n "LizardFS.org" >> petabyte_sparse_file
expect_equals "LizardFS.org" $(tail -c12 petabyte_sparse_file)
expect_less_or_equal $(lfs_dir_info realsize petabyte_sparse_file) $LIZARDFS_CHUNK_SIZE
