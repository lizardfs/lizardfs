timeout_set "3 minutes"

CHUNKSERVERS=6 \
	USE_LOOP_DISKS=YES \
	CHUNKSERVER_LABELS="0,1,2:eu|3,4,5:us" \
	MASTER_CUSTOM_GOALS="5 eu_eu: eu eu" \
	CHUNKSERVER_EXTRA_CONFIG="HDD_TEST_FREQ = 0|HDD_LEAVE_SPACE_DEFAULT = 0MiB" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_WRITE_REP_LIMIT = 2`
			`|CHUNKS_READ_REP_LIMIT = 2`
			`|REPLICATIONS_DELAY_INIT = 0`
			`|REPLICATIONS_DELAY_DISCONNECT = 0`
			`|ACCEPTABLE_DIFFERENCE = 0.0015`
			`|CHUNKS_REBALANCING_BETWEEN_LABELS = 1" \
	setup_local_empty_lizardfs info

# Create 30 chunks on eu servers and expect that three us servers are empty
cd "${info[mount0]}"
mkdir eu_files
lfssetgoal eu_eu eu_files
FILE_SIZE=1M file-generate "${info[mount0]}"/eu_files/{1..15}
assert_equals 3 $(lizardfs_rebalancing_status | awk '$2 == 0' | wc -l)

# Change goal of all our files from eu_eu to 2. Expect chunks to be spread evenly across servers
lfssetgoal -r 2 eu_files
assert_eventually_prints "" "lizardfs_rebalancing_status | awk '\$2 < 4 || \$2 > 6'"
