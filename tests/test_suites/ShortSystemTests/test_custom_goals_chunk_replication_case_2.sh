timeout_set "1 minute"

# Start an installation with 3 servers labeled 'us', 5 labeled 'eu' and the default goal "us _"
USE_RAMDISK=YES \
	CHUNKSERVERS=8 \
	CHUNKSERVER_LABELS="0,1,2:us|3,4,5,6,7:eu" \
	MASTER_CUSTOM_GOALS="1 default: us _" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|ACCEPTABLE_DIFFERENCE = 1.0`
			`|CHUNKS_WRITE_REP_LIMIT = 5`
			`|OPERATIONS_DELAY_INIT = 0`
			`|OPERATIONS_DELAY_DISCONNECT = 0" \
	setup_local_empty_lizardfs info

# Create 40 files. We will always maintain at least one copy on "us" servers.
cd "${info[mount0]}"
FILE_SIZE=1K file-generate file{1..40}

# Stop one chunkserver labeled "us" and wait for data to be replicated from spare copies.
lizardfs_chunkserver_daemon 0 stop
lizardfs_wait_for_ready_chunkservers 7
assert_eventually_prints 40 "lizardfs checkfile file* | grep 'with [2-5] copies: *1' | wc -l"

# As soon as each chunk has at least 2 copies disconnect all "eu" servers.
for csid in {3..7}; do
	lizardfs_chunkserver_daemon "$csid" kill &
done
wait
lizardfs_wait_for_ready_chunkservers 2

# Expect at least one copy for each file to survive and eventually replicate.
assert_equals 0 $(lizardfs checkfile file* | grep -i 'with 0 copies' | wc -l)
assert_eventually_prints 40 "lizardfs checkfile file* | grep 'with 2 copies: *1' | wc -l"
assert_success file-validate file*
