timeout_set 2 minutes

# Returns number of standard chunks and number of different parts for each xor level.
# If the same chunk has two copies of the same parts, these will be counted as one, eg:
# '3 standard' -- just 3 standard chunks
# '2 standard 3 xor2' -- 2 standard chunks and 3 xor2 chunks
# '6 xor5' -- 6 xor5 chunks (each is different)
chunks_state() {
	{
		find_all_chunks | grep -o chunk_.* | grep -o chunk_00000 | sed -e 's/.*/standard/'
		find_all_chunks | grep -o chunk_.* | sort -u | grep -o '_of_[2-9]' | sed -e 's/_of_/xor/'
	} | sort | uniq -c | tr '\n' ' ' | trim_hard
}

count_chunks_on_chunkservers() {
	for i in $@; do
		find_chunkserver_chunks $i
	done | wc -l
}

USE_RAMDISK=YES \
	CHUNKSERVERS=9 \
	CHUNKSERVER_LABELS="3,4,5:hdd|6,7,8:ssd" \
	MASTER_CUSTOM_GOALS="10 xor2_ssd: \$xor2 {ssd ssd ssd}`
			`|11 xor3_hdd: \$xor3 {hdd hdd hdd hdd}`
			`|12 xor5_mix: \$xor5 {hdd ssd hdd ssd}" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|CHUNKS_SOFT_DEL_LIMIT = 10`
			`|CHUNKS_WRITE_REP_LIMIT = 10`
			`|OPERATIONS_DELAY_INIT = 0`
			`|CHUNKS_REBALANCING_BETWEEN_LABELS=1`
			`|OPERATIONS_DELAY_DISCONNECT = 0"\
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
mkdir dir
lizardfs setgoal xor2_ssd dir
FILE_SIZE=1K file-generate dir/file

assert_equals "3 xor2" "$(chunks_state)"
assert_equals 3 "$(count_chunks_on_chunkservers {6..8})"
assert_equals 3 "$(count_chunks_on_chunkservers {0..8})"

lizardfs setgoal xor3_hdd dir/file
assert_eventually_prints '4 xor3' 'chunks_state' '2 minutes'
assert_equals 3 "$(count_chunks_on_chunkservers {3..5})"
assert_equals 4 "$(count_chunks_on_chunkservers {0..8})"

lizardfs setgoal xor5_mix dir/file
assert_eventually_prints '6 xor5' 'chunks_state' '2 minutes'
assert_less_or_equal 2 "$(count_chunks_on_chunkservers {3..5})"
assert_less_or_equal 2 "$(count_chunks_on_chunkservers {6..8})"
