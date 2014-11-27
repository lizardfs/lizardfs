timeout_set '5 minutes'

CHUNKSERVERS=6 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="lfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="ACCEPTABLE_DIFFERENCE = 10|CHUNKS_LOOP_TIME = 3|REPLICATIONS_DELAY_INIT = 0" \
	setup_local_empty_lizardfs info

# Create a file consisting of 2 chunks of goal 4 (8 copies in total)
cd "${info[mount0]}"
touch file
lfssetgoal 4 file
FILE_SIZE=$((1000 + LIZARDFS_CHUNK_SIZE)) file-generate file
assert_success file-validate file
assert_equals 8 $(find_all_chunks | wc -l)

# Create its snapshots in goal 1 and 2; there should be still 8 chunk files in total
lfsmakesnapshot file file_snapshot1
lfsmakesnapshot file file_snapshot2
lfssetgoal 1 file_snapshot1
lfssetgoal 2 file_snapshot2
assert_equals 8 $(find_all_chunks | wc -l)

# Remove file leaving only snapshots; number of copies should decrease to 4 (goal 2)
lfssettrashtime 0 file*
rm file
echo "Waiting for chunks to be deleted..."
assert_eventually '[[ $(lfsfileinfo file_snapshot2 | grep copy | wc -l) == 4 ]]' '3 minutes'
echo "Checking if chunks are no longer being deleted..."
assert_failure wait_for '[[ $(lfsfileinfo file_snapshot2 | grep copy | wc -l) != 4 ]]' '30 seconds'
assert_equals 4 $(lfsfileinfo file_snapshot1 | grep copy | wc -l)

# Remove file with goal 1, expect no deletions
rm file_snapshot1
echo "Checking if chunks are not being deleted..."
assert_failure wait_for '[[ $(lfsfileinfo file_snapshot2 | grep copy | wc -l) != 4 ]]' '30 seconds'

# Make a new snapshot of goal 3, expect that number of copies increases to 6
lfsmakesnapshot file_snapshot2 file_snapshot3
lfssetgoal 3 file_snapshot3
echo "Waiting for chunks to be replicated..."
assert_eventually '[[ $(lfsfileinfo file_snapshot2 | grep copy | wc -l) == 6 ]]' '30 seconds'
assert_equals 6 $(lfsfileinfo file_snapshot3 | grep copy | wc -l)

# Verify if file's data isn't damaged
assert_success file-validate file*
