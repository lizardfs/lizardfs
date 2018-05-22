timeout_set '5 minutes'

# Returns number of standard chunks and number of different parts for each xor level.
# If the same chunk has two copies of the same parts, these will be counted as one, eg:
# '3 standard' -- just 3 standard chunks
# '2 standard 3 xor2' -- 2 standard chunks and 3 xor2 chunks
# '6 xor5' -- 6 xor5 chunks (each is different)
chunks_state() {
	{
		find_all_chunks | grep -o chunk_.* | grep -o chunk_00000 | sed -e 's/.*/standard/'
		find_all_chunks | grep -o chunk_.* | sort -u | grep -o '_of_[2-9]' | sed -e 's/_of_/xor/'
	} | sort | uniq -c | tr '\n' ' ' | trim_hard
}

CHUNKSERVERS=9 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="ACCEPTABLE_DIFFERENCE = 10`
			`|CHUNKS_LOOP_TIME = 3`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|OPERATIONS_DELAY_INIT = 0" \
	setup_local_empty_lizardfs info

# Create a file consisting of 2 chunks of goal 4 (8 copies in total)
cd "${info[mount0]}"
touch file
lizardfs setgoal 4 file
FILE_SIZE=$((1000 + LIZARDFS_CHUNK_SIZE)) file-generate file
assert_success file-validate file
assert_equals "8 standard" "$(chunks_state)"

# Create its snapshots in goal 2 and xor3; chunks should be replicated
lizardfs makesnapshot file file_snapshot1
lizardfs makesnapshot file file_snapshot2
lizardfs setgoal 2 file_snapshot1
lizardfs setgoal xor3 file_snapshot2

echo "Waiting for chunks to be replicated..."
assert_eventually_prints '8 standard 8 xor3' 'chunks_state' '3 minutes'

# Remove file leaving only snapshots; there should be 4 xor3 parts created for 2 chunks (8 total)
lizardfs settrashtime 0 file*
rm file
echo "Waiting for chunks to be deleted..."
assert_eventually_prints '4 standard 8 xor3' 'chunks_state' '3 minutes'
echo "Checking if chunks are no longer being converted/deleted..."
assert_failure wait_for '[[ "$(chunks_state)" != "4 standard 8 xor3" ]]' '15 seconds'

# Create one more snapshot of goal xor5
lizardfs makesnapshot file_snapshot1 file_snapshot3
lizardfs setgoal xor5 file_snapshot3
echo "Waiting for chunks to be replicated..."
assert_eventually_prints '4 standard 8 xor3 12 xor5' 'chunks_state' '3 minutes'

# Remove file with goal 2
rm file_snapshot1
echo "Waiting for chunks to be deleted..."
assert_eventually_prints '8 xor3 12 xor5' 'chunks_state' '3 minutes'

# Make a new snapshot of goal 3, expect 6 standard chunks (2 chunks x 3 copies)
lizardfs makesnapshot file_snapshot2 file_snapshot4
lizardfs setgoal 3 file_snapshot4
echo "Waiting for chunks to be replicated..."
assert_eventually_prints '6 standard 8 xor3 12 xor5' 'chunks_state' '3 minutes'

# Verify if file's data isn't damaged
assert_success file-validate file*
