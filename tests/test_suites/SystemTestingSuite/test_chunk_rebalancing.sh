timeout_set 120 seconds
rebalancing_timeout=90

# Returns information about number of chunks on each chunkserver, eg:
# 9 9 2 1 -- one chunk of one of the chunkservers, two on another, 9 on the last two chunkservers
get_rebalancing_status() {
	find_all_chunks ! -name '*_00000000.mfs' \
			| sed -e 's|/../chunk_[0-9A-F_]*.mfs||' \
			| sort \
			| uniq -c \
			| sort -rn \
			| awk '{printf("%d ", $1)}' \
			| sed -e 's/ $//'
}

CHUNKSERVERS=4 \
	USE_LOOP_DISKS=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1|REPLICATIONS_DELAY_INIT = 0|ACCEPTABLE_DIFFERENCE = 0.003" \
	setup_local_empty_lizardfs info
	
# Create some chunks on two out of four chunkservers
mfschunkserver -c "${info[chunkserver0_config]}" stop
mfschunkserver -c "${info[chunkserver1_config]}" stop
cd "${info[mount0]}"
mkdir dir
mfssetgoal 2 dir
for i in {1..20}; do
	( FILE_SIZE=1M expect_success file-generate "dir/file_$i" ) &
done
wait
mfschunkserver -c "${info[chunkserver0_config]}" start
mfschunkserver -c "${info[chunkserver1_config]}" start

echo "Waiting for rebalancing..."
expected_rebalancing_status="10 10 10 10"
status=
end_time=$((rebalancing_timeout + $(date +%s)))
while [[ $status != $expected_rebalancing_status ]] && (( $(date +%s) < end_time )); do
	sleep 1
	status=$(get_rebalancing_status)
	echo "Rebalancing status: $status"
done
MESSAGE="Chunks are not rebalanced properly" assert_equals "$expected_rebalancing_status" "$status"

for csid in {0..3}; do
	config=${info[chunkserver${csid}_config]}
	mfschunkserver -c "${config}" stop
	MESSAGE="Validating files without chunkserver $csid" expect_success file-validate dir/*
	mfschunkserver -c "${config}" start
done
