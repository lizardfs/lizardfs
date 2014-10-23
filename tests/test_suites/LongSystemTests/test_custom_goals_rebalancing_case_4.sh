timeout_set "2 minutes"

CHUNKSERVERS=6 \
	USE_LOOP_DISKS=YES \
	CHUNKSERVER_LABELS="0,1,2,3:hdd|4,5:ssd" \
	MASTER_CUSTOM_GOALS="10 two_hdds: hdd hdd|11 two_with_hdd: hdd _" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_WRITE_REP_LIMIT = 1`
			`|CHUNKS_READ_REP_LIMIT = 2`
			`|REPLICATIONS_DELAY_INIT = 0`
			`|REPLICATIONS_DELAY_DISCONNECT = 0`
			`|ACCEPTABLE_DIFFERENCE = 0.0015`
			`|CHUNKS_REBALANCING_BETWEEN_LABELS = 1" \
	setup_local_empty_lizardfs info

lizardfs_chunkserver_daemon 3 stop
lizardfs_chunkserver_daemon 4 stop
lizardfs_chunkserver_daemon 5 stop
lizardfs_wait_for_ready_chunkservers 3

# Create some chunks on three out of four chunkservers
cd "${info[mount0]}"
for goal in two_hdds two_with_hdd; do
	mkdir $goal
	mfssetgoal $goal $goal
	FILE_SIZE=1M file-generate $goal/file_{1..12}
done

# Check if chunks are properly rebalanced now
assert_eventually_prints "" "lizardfs_rebalancing_status | awk '\$2 != 16'"

# Check if chunks are properly rebalanced after starting hdd chunkserver
lizardfs_chunkserver_daemon 3 start
assert_eventually_prints "" "lizardfs_rebalancing_status | awk '\$2 != 12'" "1 minute"

# Check if chunks are properly rebalanced after starting two ssd chunkservers
lizardfs_chunkserver_daemon 4 start
lizardfs_chunkserver_daemon 5 start
assert_eventually_prints "" "lizardfs_rebalancing_status | awk '\$2 != 9 && \$2 != 6'" "1 minute"
