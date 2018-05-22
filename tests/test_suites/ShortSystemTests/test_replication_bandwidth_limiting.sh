timeout_set "90 seconds"

replication_limit=1024
CHUNKSERVERS=2 \
	USE_RAMDISK=YES \
	CHUNKSERVER_EXTRA_CONFIG="REPLICATION_BANDWIDTH_LIMIT_KBPS=$replication_limit" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|CHUNKS_WRITE_REP_LIMIT = 1000`
			`|CHUNKS_READ_REP_LIMIT = 100`
			`|OPERATIONS_DELAY_INIT = 0`
			`|OPERATIONS_DELAY_DISCONNECT = 0" \
	setup_local_empty_lizardfs info

chunks_health() {
	lizardfs-probe chunks-health --porcelain localhost "${info[matocl]}"
}

cd "${info[mount0]}"
mkdir dir
lizardfs setgoal 2 dir
cd dir

file_size_kb=$((5 * 1024)) # test assumes that this is less or equal to chunk size
chunks_count=11
FILE_SIZE=${file_size_kb}K file-generate $(seq 1 $chunks_count)

assert_equals $chunks_count $(find_chunkserver_chunks 0 | wc -l)

health_ok=$(chunks_health)
lizardfs_chunkserver_daemon 0 stop

find_chunkserver_chunks 0 | xargs -d'\n' rm -f
assert_equals 0 $(find_chunkserver_chunks 0 | wc -l)
assert_equals $chunks_count $(find_chunkserver_chunks 1 | wc -l)

lizardfs_chunkserver_daemon 0 start

start_TS=$(timestamp)
expected_time_s=$((file_size_kb * chunks_count / replication_limit))
accepted_inaccuracy_s=5
if valgrind_enabled; then
	accepted_inaccuracy_s=30
fi;
assert_success wait_for \
		'[ "$health_ok" == "$(chunks_health)" ]' \
		"$((expected_time_s + accepted_inaccuracy_s)) seconds"
end_TS=$(timestamp)
assert_near $expected_time_s $((end_TS - start_TS)) $accepted_inaccuracy_s

