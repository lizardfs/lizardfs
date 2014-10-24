timeout_set 120 seconds
rebalancing_timeout=90

CHUNKSERVERS=4 \
	USE_LOOP_DISKS=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_EXTRA_CONFIG="HDD_TEST_FREQ = 0|HDD_LEAVE_SPACE_DEFAULT = 0MiB" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_WRITE_REP_LIMIT = 1`
			`|CHUNKS_READ_REP_LIMIT = 2`
			`|REPLICATIONS_DELAY_INIT = 0`
			`|REPLICATIONS_DELAY_DISCONNECT = 0`
			`|ACCEPTABLE_DIFFERENCE = 0.0015" \
	setup_local_empty_lizardfs info

# Create some chunks on two out of four chunkservers
lizardfs_chunkserver_daemon 0 stop
lizardfs_chunkserver_daemon 1 stop
lizardfs_wait_for_ready_chunkservers 2
cd "${info[mount0]}"
mkdir dir
mfssetgoal 2 dir
for i in {1..20}; do
	( FILE_SIZE=1M expect_success file-generate "dir/file_$i" ) &
done
wait
lizardfs_chunkserver_daemon 0 start
lizardfs_chunkserver_daemon 1 start

echo "Waiting for rebalancing..."
expected_rebalancing_status="10 10 10 10"
status=
end_time=$((rebalancing_timeout + $(date +%s)))
while [[ $status != $expected_rebalancing_status ]] && (( $(date +%s) < end_time )); do
	sleep 1
	status=$(lizardfs_rebalancing_status | awk '{print $2}' | xargs echo)
	echo "Rebalancing status: $status"
done
MESSAGE="Chunks are not rebalanced properly" assert_equals "$expected_rebalancing_status" "$status"

for csid in {0..3}; do
	lizardfs_chunkserver_daemon $csid stop
	MESSAGE="Validating files without chunkserver $csid" expect_success file-validate dir/*
	lizardfs_chunkserver_daemon $csid start
	lizardfs_wait_for_ready_chunkservers 4
done
