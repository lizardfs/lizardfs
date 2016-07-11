timeout_set "3 minutes"

CHUNKSERVERS=5 \
	USE_LOOP_DISKS=YES \
	CHUNKSERVER_LABELS="0,1,2:ssd|3,4:hdd" \
	MASTER_CUSTOM_GOALS="1 default: ssd _" \
	CHUNKSERVER_EXTRA_CONFIG="HDD_TEST_FREQ = 0|HDD_LEAVE_SPACE_DEFAULT = 0MiB" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_WRITE_REP_LIMIT = 2`
			`|CHUNKS_READ_REP_LIMIT = 2`
			`|OPERATIONS_DELAY_INIT = 0`
			`|OPERATIONS_DELAY_DISCONNECT = 0`
			`|ACCEPTABLE_DIFFERENCE = 0.0015`
			`|CHUNKS_REBALANCING_BETWEEN_LABELS = 1" \
	setup_local_empty_lizardfs info

# Shut down one 'ssd' server leaving 2 x 'hdd' and 2 x 'ssd' running
lizardfs_chunkserver_daemon 0 stop
lizardfs_wait_for_ready_chunkservers 4

# Create 200 chunks on our four chunkservers. Expect about 50 chunks to be located on each server.
FILE_SIZE=1M file-generate "${info[mount0]}"/file{1..100}
assert_eventually_prints "" "lizardfs_rebalancing_status | awk '\$2 < 47 || \$2 > 53'" "1 minute"

# Add one 'ssd' server and expect chunks to be rebalanced to about 40 on each server.
lizardfs_chunkserver_daemon 0 start
assert_eventually_prints "" "lizardfs_rebalancing_status | awk '\$2 < 38 || \$2 > 42'" "90 seconds"
