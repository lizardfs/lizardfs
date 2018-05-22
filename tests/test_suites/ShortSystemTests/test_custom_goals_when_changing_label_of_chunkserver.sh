# Start an installation with 2 servers labeled 'us', 2 labeled 'eu' and the default goal "us"
USE_RAMDISK=YES \
	CHUNKSERVERS=4 \
	CHUNKSERVER_LABELS="0,1:us|2,3:eu" \
	MASTER_CUSTOM_GOALS="1 default: us" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|ACCEPTABLE_DIFFERENCE = 1.0`
			`|CHUNKS_WRITE_REP_LIMIT = 5`
			`|OPERATIONS_DELAY_INIT = 0`
			`|OPERATIONS_DELAY_DISCONNECT = 0" \
	setup_local_empty_lizardfs info

# Create 20 files. All chunks should be placed on chunkservers 0 and 1.
cd "${info[mount0]}"
FILE_SIZE=1K file-generate file{1..20}
assert_equals 0 $(find_chunkserver_chunks 2 | wc -l)
assert_equals 0 $(find_chunkserver_chunks 3 | wc -l)

# Change label of chunkserver 0 from "us" to "eu".
sed -i -re 's/LABEL ?=.*/LABEL = eu/' "${info[chunkserver0_cfg]}"
lizardfs_chunkserver_daemon 0 reload

# Expect all chunks to disappear from this server and move to server 1.
assert_eventually_prints 0 'find_chunkserver_chunks 0 | wc -l'
assert_equals 20 $(find_chunkserver_chunks 1 | wc -l)
assert_equals  0 $(find_chunkserver_chunks 2 | wc -l)
assert_equals  0 $(find_chunkserver_chunks 3 | wc -l)
