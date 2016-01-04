timeout_set 2 minutes

# Returns number of standard chunks and number of different parts for each ec level.
# If the same chunk has two copies of the same parts, these will be counted as one, eg:
# '3 standard' -- just 3 standard chunks
# '2 standard 3 ec(2,1)' -- 2 standard chunks and 3 ec(2,1) parts
# '6 ec(3,2)' -- 6 ec(3,2) parts (each is different)
chunks_state() {
	{
		find_all_chunks | grep -o chunk_.* | grep -o chunk_00000 | sed -e 's/.*/standard/'
		find_all_chunks | grep -o chunk_.* | sort -u | grep -o '_of_[2-9]_[1-9]' | sed -e 's/_of_/ec/'
	} | sort | uniq -c | tr '\n' ' ' | trim_hard
}

count_chunks_on_chunkservers() {
	for i in $@; do
		find_chunkserver_chunks $i
	done | wc -l
}

USE_RAMDISK=YES \
	CHUNKSERVERS=12 \
	CHUNKSERVER_LABELS="3,4,5:hdd|6,7,8:ssd|9,10,11:floppy" \
	MASTER_CUSTOM_GOALS="10 ec21_ssd: \$ec(2,1){ssd ssd ssd}`
			`|11 ec33_hdd: \$ec(3,3) {hdd hdd hdd hdd}`
			`|12 ec22_mix: \$ec(2,2) {hdd ssd hdd ssd}`
			`|13 ec36_mix: \$ec(3,6) {ssd ssd ssd floppy floppy floppy hdd hdd hdd}" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_SOFT_DEL_LIMIT = 10`
			`|CHUNKS_WRITE_REP_LIMIT = 10`
			`|REPLICATIONS_DELAY_INIT = 0`
			`|REPLICATIONS_DELAY_DISCONNECT = 0"\
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
mkdir dir
lizardfs setgoal ec21_ssd dir
FILE_SIZE=1K file-generate dir/file

assert_equals "3 ec2_1" "$(chunks_state)"
assert_equals 3 "$(count_chunks_on_chunkservers {6..8})"
assert_equals 3 "$(count_chunks_on_chunkservers {0..11})"

lizardfs setgoal ec33_hdd dir/file
assert_eventually_prints '6 ec3_3' 'chunks_state' '2 minutes'
assert_equals 3 "$(count_chunks_on_chunkservers {3..5})"
assert_equals 6 "$(count_chunks_on_chunkservers {0..11})"

lizardfs setgoal ec22_mix dir/file
assert_eventually_prints '4 ec2_2' 'chunks_state' '2 minutes'
assert_equals 2 "$(count_chunks_on_chunkservers {3..5})"
assert_equals 2 "$(count_chunks_on_chunkservers {6..8})"

lizardfs setgoal ec36_mix dir/file
lizardfs fileinfo dir/*
assert_eventually_prints '9 ec3_6' 'chunks_state' '2 minutes'
lizardfs fileinfo dir/*
assert_equals 3 "$(count_chunks_on_chunkservers {3..5})"
assert_equals 3 "$(count_chunks_on_chunkservers {6..8})"
assert_equals 3 "$(count_chunks_on_chunkservers {9..11})"
