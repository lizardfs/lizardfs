timeout_set "1 minute"

# Start an installation with 2 servers labeled 'hdd', 2 labeled 'ssd' and the default goal "ssd ssd"
USE_RAMDISK=YES \
	CHUNKSERVERS=4 \
	CHUNKSERVER_LABELS="0,1:ssd|2,3:hdd" \
	MASTER_CUSTOM_GOALS="1 default: ssd ssd" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|ACCEPTABLE_DIFFERENCE = 1.0`
			`|CHUNKS_WRITE_REP_LIMIT = 5`
			`|OPERATIONS_DELAY_INIT = 0`
			`|ENDANGERED_CHUNKS_PRIORITY = 0.7`
			`|OPERATIONS_DELAY_DISCONNECT = 0" \
	setup_local_empty_lizardfs info

# Leave only one "hdd" and one "ssd" server.
lizardfs_chunkserver_daemon 1 stop
lizardfs_chunkserver_daemon 2 stop
lizardfs_wait_for_ready_chunkservers 2

# Create 20 files. Expect that for each file there are 2 chunk copies.
FILE_SIZE=1K file-generate "${info[mount0]}"/file{1..20}
assert_equals 20 $(lizardfs checkfile "${info[mount0]}"/* | grep 'with 2 copies: *1' | wc -l)

# Stop the chunkserver labeled "ssd" and expect all files to have a chunk in only one copy.
assert_equals 20 $(find_chunkserver_chunks 0 | wc -l)
lizardfs_chunkserver_daemon 0 stop
lizardfs_wait_for_ready_chunkservers 1
assert_equals 20 $(lizardfs checkfile "${info[mount0]}"/* | grep 'with 1 copy: *1' | wc -l)

# Add one "hdd" chunkserver. Expect that second copy of each chunk will be created there.
lizardfs_chunkserver_daemon 2 start
lizardfs_wait_for_ready_chunkservers 2
assert_eventually_prints 20 'lizardfs checkfile "${info[mount0]}"/* | grep "with 2 copies: *1" | wc -l'

# Remove all chunks from the chunkserver "ssd" and bring it back to life.
find_chunkserver_chunks 0 | xargs -d'\n' rm -f
assert_equals 0 $(find_chunkserver_chunks 0 | wc -l)
lizardfs_chunkserver_daemon 0 start
lizardfs_wait_for_ready_chunkservers 3

# Expect one copy of each chunk to migrate to the "ssd" server.
assert_eventually_prints 20 'find_chunkserver_chunks 0 | wc -l'

# No chunks should be deleted until we have two "ssd" servers. So let's add one.
assert_eventually_prints 60 'find_all_chunks | wc -l'
lizardfs_chunkserver_daemon 1 start
lizardfs_wait_for_ready_chunkservers 4
assert_eventually_prints 20 'find_chunkserver_chunks 1 | wc -l'
assert_eventually_prints 20 'find_chunkserver_chunks 0 | wc -l'
assert_eventually_prints 40 'find_all_chunks | wc -l'
