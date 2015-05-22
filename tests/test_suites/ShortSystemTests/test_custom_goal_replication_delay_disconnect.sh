timeout_set "1 minute"

# Start an installation with:
#   two servers labeled 'ssd' and 2 unlabeled servers
#   the default goal "ssd _"
USE_RAMDISK=YES \
	CHUNKSERVERS=4 \
	CHUNKSERVER_LABELS="0,1:ssd" \
	MASTER_CUSTOM_GOALS="1 default: ssd _" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|ACCEPTABLE_DIFFERENCE = 1.0`
			`|CHUNKS_WRITE_REP_LIMIT = 5`
			`|REPLICATIONS_DELAY_INIT = 0`
			`|REPLICATIONS_DELAY_DISCONNECT = 15" \
	setup_local_empty_lizardfs info

# Leave only one unlabeled and one 'ssd' server
lizardfs_chunkserver_daemon 1 stop
lizardfs_chunkserver_daemon 3 stop
lizardfs_wait_for_ready_chunkservers 2

# Wait few seconds to avoid unwanted replication delay.
sleep 17

# Create 20 files. Expect that for each file there are 2 chunk copies.
FILE_SIZE=1K file-generate "${info[mount0]}"/file{1..20}
assert_equals 20 $(mfscheckfile "${info[mount0]}"/* | grep 'with 2 copies:' | wc -l)

# Stop 'ssd' server and start unlabeled server.
# We have only two unlabeled servers now.
lizardfs_chunkserver_daemon 0 stop
lizardfs_chunkserver_daemon 3 start
lizardfs_wait_for_ready_chunkservers 2

# All chunks has 1 missing replica on 'ssd' server
# but they never should be replicated to some random server.
assert_equals 20 $(mfscheckfile "${info[mount0]}"/* | grep 'with 1 copy:' | wc -l)
sleep 7
assert_equals 20 $(mfscheckfile "${info[mount0]}"/* | grep 'with 1 copy:' | wc -l)

# Restart 'ssd' server.
lizardfs_chunkserver_daemon 1 start
lizardfs_wait_for_ready_chunkservers 3

# Replication should start immediately
replication_timeout="4 seconds"
if valgrind_enabled; then
	replication_timeout="7 seconds"
fi
assert_eventually_prints 20 'find_chunkserver_chunks 1 | wc -l' "$replication_timeout"
assert_equals 20 $(mfscheckfile "${info[mount0]}"/* | grep 'with 2 copies:' | wc -l)
