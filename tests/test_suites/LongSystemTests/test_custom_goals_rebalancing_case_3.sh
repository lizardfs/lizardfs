timeout_set "6 minutes"

CHUNKSERVERS=6 \
	USE_LOOP_DISKS=YES \
	CHUNKSERVER_LABELS="0,1,2:eu|3,4,5:us" \
	MASTER_CUSTOM_GOALS="5 eu_eu: eu eu" \
	CHUNKSERVER_EXTRA_CONFIG="PERFORM_FSYNC = 1|HDD_TEST_FREQ = 10000|HDD_LEAVE_SPACE_DEFAULT = 0MiB" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_MIN_TIME = 10`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|CHUNKS_WRITE_REP_LIMIT = 2`
			`|CHUNKS_READ_REP_LIMIT = 2`
			`|CHUNKS_SOFT_DEL_LIMIT = 5`
			`|CHUNKS_HARD_DEL_LIMIT = 5`
			`|OPERATIONS_DELAY_INIT = 5`
			`|OPERATIONS_DELAY_DISCONNECT = 5`
			`|ACCEPTABLE_DIFFERENCE = 0.005`
			`|CHUNKS_REBALANCING_BETWEEN_LABELS = 1" \
	setup_local_empty_lizardfs info

# Create 300 chunks on eu servers and expect that three us servers are empty
cd "${info[mount0]}"
mkdir eu_files
lizardfs setgoal eu_eu eu_files
FILE_SIZE=1M file-generate "${info[mount0]}"/eu_files/{1..150}
assert_eventually_prints 3 "lizardfs_rebalancing_status | awk '/eu/ && \$2 > 0' | wc -l" "1 minute"
assert_equals 3 $(lizardfs_rebalancing_status | awk '/us/ && $2 == 0' | wc -l)

# Change goal of all our files from eu_eu to 2. Expect chunks to be spread evenly across servers
lizardfs setgoal -r 2 eu_files
assert_eventually_prints "" "lizardfs_rebalancing_status | awk '\$2 < 40 || \$2 > 60'" "2 minutes"

# Check the chunkservers load after 5 seconds to see if it is stable.
sleep 5
assert_awk_finds_no '$2 < 40 || $2 > 60' "$(lizardfs_rebalancing_status)"
