timeout_set "5 minutes"

# Create a file where all chunk replications would be reported
replication_log=$TEMP_DIR/log
touch "$replication_log"

# Prints the current human-readable status of chunkservers (disk usage, number of chunks, etc)
status() {
	lizardfs-probe list-chunkservers localhost "${info[matocl]}"
}

# Set up an installation with 6 chunkservers on loop disks, with 4 different labels.
# The default goal is to keep 2 copies, at least one copy on some 'eu' server.
# We accept 40 MB of difference in disk usage.
# HDD_TEST_FREQ is set to 0 to reduce disk load in this test.
CHUNKSERVERS=6 \
	USE_LOOP_DISKS=YES \
	CHUNKSERVER_LABELS="0:us|1,2,3:eu|4:au|5:cn" \
	MASTER_CUSTOM_GOALS="1 default: eu _" \
	CHUNKSERVER_EXTRA_CONFIG="HDD_TEST_FREQ = 0`
			`|HDD_LEAVE_SPACE_DEFAULT = 0MiB`
			`|MAGIC_DEBUG_LOG = cs.matocs.replicate:$TEMP_DIR/log" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|REPLICATIONS_DELAY_INIT = 0`
			`|REPLICATIONS_DELAY_DISCONNECT = 0`
			`|ACCEPTABLE_DIFFERENCE = 0.03`
			`|CHUNKS_REBALANCING_BETWEEN_LABELS = 1" \
	setup_local_empty_lizardfs info

# Stop the 'cn' chunkserver in the initial phase -- leave 1 x 'us', 3 x 'eu' and 1 x 'au' running
lizardfs_chunkserver_daemon 5 stop
lizardfs_wait_for_ready_chunkservers 5

# Create 300 files, 1 MB each -- realsize would be 600 MB. Expect no replications to happen!
for i in {1..30}; do
	MESSAGE="Step $i/100, status:"$'\n'"$(status)"$'\nCreating new files'
	FILE_SIZE=1M file-generate "${info[mount0]}/file_${i}_"{1..10}
	assert_awk_finds_no '/cs.matocs.replicate/' "$(cat "$replication_log")"
done

# Verify if rebalancing would start when we add one empty server labelled 'cn'
MESSAGE=$'Status:\n'"$(status)"$'\nWaiting for data migration to China'
lizardfs_chunkserver_daemon 5 start
assert_eventually '[[ $(awk "/cs.matocs.replicate/" "$replication_log" | wc -l) != 0 ]]'
