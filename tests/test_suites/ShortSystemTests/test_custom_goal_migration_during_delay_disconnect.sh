timeout_set "1 minute"

# Start an installation with:
#   one server labeled 'raid', 2 servers labeled 'something', and 1 unlabeled server
#   the default goal "_ _" and additional goal "raid _ _"
USE_RAMDISK=YES \
	CHUNKSERVERS=4 \
	CHUNKSERVER_LABELS="0:raid|2,3:something" \
	MASTER_CUSTOM_GOALS="1 default: _ _|2 raid_goal: raid _ _" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|ACCEPTABLE_DIFFERENCE = 1.0`
			`|CHUNKS_WRITE_REP_LIMIT = 5`
			`|OPERATIONS_DELAY_INIT = 0`
			`|OPERATIONS_DELAY_DISCONNECT = 40" \
	setup_local_empty_lizardfs info

# Leave only one unlabeled and one 'something' servers
lizardfs_chunkserver_daemon 0 stop
lizardfs_chunkserver_daemon 1 stop
lizardfs_wait_for_ready_chunkservers 2

# Create 20 files. Expect that for each file there are 2 chunk copies.
FILE_SIZE=1K file-generate "${info[mount0]}"/file{1..20}
assert_equals 20 $(lizardfs checkfile "${info[mount0]}"/* | grep 'with 2 copies:' | wc -l)

# Stop one server with valid copy and start two new servers.
lizardfs_chunkserver_daemon 2 stop
lizardfs_chunkserver_daemon 0 start
lizardfs_chunkserver_daemon 1 start
lizardfs_wait_for_ready_chunkservers 3

# All chunks has 1 missing copy but replication shouldn't be started.
assert_equals 20 $(lizardfs checkfile "${info[mount0]}"/* | grep 'with 1 copy:' | wc -l)
sleep 10
assert_equals 20 $(lizardfs checkfile "${info[mount0]}"/* | grep 'with 1 copy:' | wc -l)

# Change goal. From now all chunks have 1 missing copy on 'raid' server and 1 on some other server.
lizardfs setgoal raid_goal "${info[mount0]}"/* > /dev/null

# Chunks should be replicated only on 'raid' server.
# Replication to other servers is delayed because of disconnected chunkserver 2.
assert_eventually_prints 20 'find_chunkserver_chunks 0 | wc -l' '5 seconds'
assert_equals 20 $(lizardfs checkfile "${info[mount0]}"/* | grep 'with 2 copies:' | wc -l)

# Replication shouldn't be started for few more seconds.
sleep 10
assert_equals 20 $(lizardfs checkfile "${info[mount0]}"/* | grep 'with 2 copies:' | wc -l)
assert_equals 0 $(find_chunkserver_chunks 1 | wc -l)

# Expect one copy of each chunk to migrate to the last unlabeled server.
assert_eventually_prints 20 'find_chunkserver_chunks 1 | wc -l' '20 seconds'
assert_equals 20 $(lizardfs checkfile "${info[mount0]}"/* | grep 'with 3 copies:' | wc -l)
