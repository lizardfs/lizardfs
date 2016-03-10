CHUNKSERVERS=1 \
	CHUNKSERVER_EXTRA_CONFIG="HDD_PUNCH_HOLES = 1" \
	setup_local_empty_lizardfs info

file=$(mktemp -p ${info[mount0]})
hdd=$(cat "${info[chunkserver0_hdd]}")

test_fallocate() {
(
echo "#define _GNU_SOURCE"
echo "#include <fcntl.h>"
echo "int main() {"
echo "  int fd = 1;"
echo "  fallocate(fd, FALLOC_FL_PUNCH_HOLE, 0, 1024);"
echo "  return 0;"
echo "}"
) | gcc -o $TEMP_DIR/punch_test.o -xc -
}

if ! test_fallocate; then
	test_end
fi

dd if=/dev/urandom of=$file count=16 bs=1024 conv=fsync

sleep 1
chunk_file=$(find "$hdd" -name 'chunk_*.???')
full_size=$(stat -c "%b" "$chunk_file")

dd if=/dev/zero of=$file count=6 bs=1024 seek=3 conv=fsync

sleep 1
chunk_file=$(find "$hdd" -name 'chunk_*.???')
sparse_size=$(stat -c "%b" "$chunk_file")

if (( $sparse_size >= $full_size )); then
	test_add_failure "File is not sparse!"
fi

# Test if file with punched whole is read correctly.
dd if=/dev/urandom of=$TEMP_DIR/test_punch_hole.bin count=16 bs=1024 conv=fsync
cp $TEMP_DIR/test_punch_hole.bin ${info[mount0]}/test_punch_hole.bin

dd if=/dev/zero of=$TEMP_DIR/test_punch_hole.bin count=10 bs=1024 seek=3 conv=fsync
dd if=/dev/zero of=${info[mount0]}/test_punch_hole.bin count=10 bs=1024 seek=3 conv=fsync

cmp $TEMP_DIR/test_punch_hole.bin ${info[mount0]}/test_punch_hole.bin
