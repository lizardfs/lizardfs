timeout_set 4 minutes

CHUNKSERVERS=4 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|OPERATIONS_DELAY_INIT = 0" \
	USE_RAMDISK="YES" \
	setup_local_empty_lizardfs info

# Create a file consising of a couple of chunks and remove it
file="${info[mount0]}/file"
xorfile="${info[mount0]}/xorfile"
touch "$file" "$xorfile"
lizardfs setgoal 3 "$file"
lizardfs setgoal xor3 "$xorfile"
dd if=/dev/zero of="$file" bs=1MiB count=130
dd if=/dev/zero of="$xorfile" bs=1MiB count=130
lizardfs settrashtime 0 "$file" "$xorfile"
rm -f "$file" "$xorfile"

# Wait for removing all the chunks
timeout="3 minutes"
if ! wait_for '[[ $(find_all_chunks | wc -l) == 0 ]]' "$timeout"; then
	test_add_failure $'The following chunks were not removed:\n'"$(find_all_chunks)"
fi
