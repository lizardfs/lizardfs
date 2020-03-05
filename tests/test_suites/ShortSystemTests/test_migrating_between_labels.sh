timeout_set 1 minute

count_chunks_on_chunkservers() {
	for i in $@; do
		find_chunkserver_chunks $i
	done | wc -l
}

USE_RAMDISK=YES \
	CHUNKSERVERS=9 \
	CHUNKSERVER_LABELS="3,4,5:hdd|6,7,8:floppy" \
	MASTER_CUSTOM_GOALS="10 two_hdds: hdd hdd|11 two_flops: floppy floppy" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_MIN_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|CHUNKS_SOFT_DEL_LIMIT = 10`
			`|CHUNKS_WRITE_REP_LIMIT = 10`
			`|OPERATIONS_DELAY_INIT = 0`
			`|OPERATIONS_DELAY_DISCONNECT = 0"\
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

# Create some chunks on non-labeled chunkservers only
for i in {3..8}; do
	lizardfs_chunkserver_daemon $i stop &
done
wait
lizardfs_wait_for_ready_chunkservers 3
mkdir dir
lizardfs setgoal 2 dir
FILE_SIZE=1K file-generate dir/file{1..10}

# Turn on all chunkservers
for i in {3..8}; do
	lizardfs_chunkserver_daemon $i start &
done
wait

# Change goal of all files, verify
lizardfs setgoal two_hdds dir/file*
expect_eventually_prints 20 'count_chunks_on_chunkservers {3..5}'
assert_eventually_prints 20 'find_all_chunks | wc -l'

# Change goal of some files, verify
lizardfs setgoal two_flops dir/file{1..4}
expect_eventually_prints 8 'count_chunks_on_chunkservers {6..8}'
expect_eventually_prints 12 'count_chunks_on_chunkservers {3..5}'
assert_eventually_prints 20 'find_all_chunks | wc -l'
