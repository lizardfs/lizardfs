timeout_set 3 hours

source test_utils/test_xor_consistency_utils.sh

CHUNKSERVERS=3
MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info
dir="${info[mount0]}/dir"
mkdir "$dir"
lizardfs setgoal xor2 "$dir"

DATA_SIZE_PER_THREAD=$(parse_si_suffix 3G)
thread_count=10

echo "Writing data to files"
for i in $(seq 1 $thread_count); do
	writing_loop_thread "$dir" $i &
done
wait

echo "Overwriting data in files"
for i in $(seq 1 $thread_count); do
	overwriting_loop_thread "$dir" $i &
done
wait

if [ ${FAULTY_MASTER:-} ]; then
	master_restarting_loop &
fi
if [ ${FAULTY_CHUNKSERVERS:-} ]; then
	chunkservers_restarting_loop $CHUNKSERVERS &
fi

echo "Verifying the files contain the expected data when read"
thread_pids=()
for i in $(seq 1 $thread_count); do
	verifying_loop_thread "$dir" $i &
	thread_pids[i]=$!
done
wait ${thread_pids[@]}

if [ ${FAULTY_MASTER:-} ]; then
	stop_master_restarting_thread
fi
if [ ${FAULTY_CHUNKSERVERS:-} ]; then
	stop_chunkservers_restarting_thread
fi
wait
