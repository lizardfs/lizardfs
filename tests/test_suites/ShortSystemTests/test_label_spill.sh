timeout_set 2 minutes

count_chunks_on_chunkservers() {
	for i in $@; do
		find_chunkserver_chunks $i
	done | wc -l
}

USE_RAMDISK=YES \
	CHUNKSERVERS=6 \
	CHUNKSERVER_LABELS="0,1,2:hdd|3,4:floppy|5:_" \
	MASTER_CUSTOM_GOALS="10 three_hdds: hdd hdd hdd" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_MIN_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|CHUNKS_SOFT_DEL_LIMIT = 10`
			`|CHUNKS_WRITE_REP_LIMIT = 10`
			`|OPERATIONS_DELAY_INIT = 0`
			`|OPERATIONS_DELAY_DISCONNECT = 0"\
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

# Stop the _ chunkserver
lizardfs_chunkserver_daemon 5 stop
lizardfs_wait_for_ready_chunkservers 5

# Create files with 3x hdd goal
mkdir dir
lizardfs setgoal three_hdds dir
FILE_SIZE=32K file-generate dir/file{1..10}

expect_eventually_prints 30 'count_chunks_on_chunkservers {0..2}'
assert_eventually_prints 30 'find_all_chunks | wc -l'

# Stop one of hdd chunkservers
lizardfs_chunkserver_daemon 2 stop
lizardfs_wait_for_ready_chunkservers 4

# Chunks should not be replicated across CHUNKSERVER_LABELS
for x in {1..16}; do
	assert_equals 0 $(count_chunks_on_chunkservers {3,4})
	sleep 1
done

# Start the _ chunkserver
lizardfs_chunkserver_daemon 5 start
lizardfs_wait_for_ready_chunkservers 5

# Chunks should be replicated to matching wildcard label (_)
expect_eventually_prints 30 'count_chunks_on_chunkservers {0..2}' '1 minute'
assert_eventually_prints 30 'find_all_chunks | wc -l' '1 minute'
