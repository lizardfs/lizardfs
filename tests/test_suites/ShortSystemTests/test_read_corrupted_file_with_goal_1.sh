CHUNKSERVERS=1 \
		MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
		CHUNKSERVER_EXTRA_CONFIG="HDD_TEST_FREQ = 10000" \
		USE_RAMDISK=YES \
		setup_local_empty_lizardfs info

cd "${info[mount0]}"
FILE_SIZE=1234567 file-generate file

find_chunkserver_chunks 0 | xargs -d'\n' -IXX \
		dd if=/dev/zero of=XX bs=1 count=4 seek=100k conv=notrunc

if timeout -s KILL 3s file-validate file; then
	test_add_failure "False positive file read"
fi
