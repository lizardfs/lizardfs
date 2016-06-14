CHUNKSERVERS=4 \
	USE_RAMDISK=YES \
	MOUNTS=2 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

# This test uses 2 mounts. We will use mount 0 to write files and mount 1 to query
# for their size to get the real information from the master server, not a size cached
# by the mountpoint which is used for writing.

# Create a directory with a goal of xor3
mkdir "${info[mount0]}/dir"
lizardfs setgoal xor3 "${info[mount0]}/dir"
stripe_size=$((3 * LIZARDFS_BLOCK_SIZE))

# Create a background process which writes 1.5 stripes to some file and then sleeps forever
# Expect the first stripe to be written immediately and the rest to be written after some time.
file_size=$((stripe_size * 3/2))
( head -c "$file_size" /dev/zero ; sleep 60 ) | dd bs=1024 of="${info[mount0]}/dir/f1" &
expect_eventually_prints "$stripe_size" 'stat -c %s "${info[mount1]}/dir/f1"' '4 seconds'
expect_eventually_prints "$file_size"  'stat -c %s "${info[mount1]}/dir/f1"' '7 seconds'

# Create a background process which constantly writes 1 byte every second to a file.
# Expect no data to be written to chunkservers within the first seconds (up to 15, let's say 10)
# But expect that after some time at least a couple of characters is written.
( while sleep 1; do echo -n x ; done ) | dd bs=1 of="${info[mount0]}/dir/f2" &
sleep 10
expect_equals 0 "$(stat -c %s "${info[mount1]}/dir/f2")"
expect_eventually_prints "xxxxx" 'head -c 5 "${info[mount0]}/dir/f2"'

# Kill background processes before exit to avoid false negatives from valgrind
if [[ ${USE_VALGRIND} ]]; then
	jobs -p | xargs kill -KILL
	assert_eventually_prints "" "jobs -p"
fi
