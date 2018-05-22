CHUNKSERVERS=4 \
	USE_RAMDISK="yes" \
	MOUNT_EXTRA_CONFIG="mfscachemode=never" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|OPERATIONS_DELAY_INIT = 0" \
	CHUNKSERVER_EXTRA_CONFIG="HDD_TEST_FREQ = 10000" \
	setup_local_empty_lizardfs info

damage_start=100K
damage_length=12

get_damaged_area() {
	dd bs=1 skip=$damage_start count=$damage_length if="$1" 2>/dev/null | base64
}

# Create a file
cd "${info[mount0]}"
touch file
lizardfs setgoal xor3 file
FILE_SIZE=1234567 file-generate file
assert_equals 4 $(lizardfs fileinfo file | grep -c copy)

# Locate part 1 of its chunk and remember correct content of the area to be damaged
chunk=$(find_all_chunks -name "*xor_1_*")
assert_equals 1 $(wc -l <<< "$chunk")
correct_data=$(get_damaged_area "$chunk")

# Damage it a bit and read the file to trigger a fix
dd if=/dev/urandom of="$chunk" bs=1 seek=$damage_start count=$damage_length conv=notrunc oflag=nocache,sync

# Read file several times to raise the probability of reading corrupted part
for x in {1..32}; do
	MESSAGE="Reading file with corrupted chunk" expect_success file-validate file
done

echo "Waiting for the chunk to be fixed..."
assert_success wait_for '[[ $(get_damaged_area "$chunk") == $correct_data ]]' "25 seconds"
assert_success wait_for '[[ $(lizardfs fileinfo file | grep -c copy) == 4 ]]' "5 seconds"
MESSAGE="Reading file with fixed chunk" expect_success file-validate file
