timeout_set 1 minute

CHUNKSERVERS=1 \
	MOUNTS=1 \
	USE_RAMDISK="YES" \
setup_local_empty_lizardfs info

cd "${info[mount0]}"

output=test_file.txt

echo 1 > $output
echo 2 >> $output
find
echo 3 >> $output
echo 4 >> $output
echo 5 >> $output

assert_equals 5 $(cat $output | wc -l)
