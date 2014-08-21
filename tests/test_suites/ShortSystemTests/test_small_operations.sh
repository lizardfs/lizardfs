timeout_set 10 minutes

CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

for block_size in 1 5 10 50 100 300 500
do
	echo "Testing writes with block size ${block_size} B"
	BLOCK_SIZE=$block_size FILE_SIZE=500K file-generate file
	file-validate file || test_add_failure \
			"Failed to create a consistent file using blocks of $block_size B"
done

FILE_SIZE=70M file-generate file
for block_size in 1 5 10 50 100 300 500
do
	echo "Testing reads with block size ${block_size} B"
	BLOCK_SIZE=$block_size file-validate file || test_add_failure \
			"Failed to read a file using blocks of $block_size B"
done
