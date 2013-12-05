CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

file=$(mktemp -p ${info[mount0]})

off1=$((5 * LIZARDFS_BLOCK_SIZE))
off2=$((2 * LIZARDFS_CHUNK_SIZE + 3 * LIZARDFS_BLOCK_SIZE))
for off in $off1 $off2; do
	dd if=/dev/zero of=$file count=1 bs=1 seek=$off &>/dev/null

	actual_length=$(mfs_dir_info length $file)
	expected_length=$((off + 1))
	if (( actual_length != expected_length )); then
		test_add_failure "Wrong length for offset $off, expected $expected_length got $actual_length!"
	fi
done

actual_size=$(mfs_dir_info size $file)
if (( $actual_size >= $actual_length )); then
	test_add_failure "File is not sparse!"
fi

exit_code=0
diff $file <(head -c $actual_length /dev/zero) || exit_code=$?
if (( exit_code != 0 )); then
	test_add_failure "File inconsistent, head: $(head -n2 $file | cut -c -100 | tr '\n' '|')"
fi
