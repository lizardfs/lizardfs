timeout_set 5 minutes

MASTER_RESTART_DELAY_SECS=5
READDIR_SLEEP_SECS=15

: ${THREAD1_FILE:=${TEMP_DIR}/.readdir_test_thread1_file}
: ${THREAD2_FILE:=${TEMP_DIR}/.readdir_test_thread2_file}
: ${THREAD3_FILE:=${TEMP_DIR}/.readdir_test_thread3_file}
: ${THREAD4_FILE:=${TEMP_DIR}/.readdir_test_thread4_file}
THREAD_COUNT=4

master_restarting_loop() {
	local restarts=$1
	for i in $(seq 1 $restarts); do
		sleep ${MASTER_RESTART_DELAY_SECS}
		expect_success lizardfs_master_daemon restart
	done
}

thread() {
	local dir=$1
	local file_count=$2
	local result_file=$3

	mkdir -p "$dir" && cd "$dir"
	touch $(seq 1 $file_count)

	local files_iterated=$(python3 - <<-END_OF_SCRIPT
	import os, itertools, time
	slice_size = int(${file_count}) // 2
	dirents = 0
	with os.scandir("${dir}") as dir:
	    dirents += len(list(itertools.islice(dir, slice_size)))
	    time.sleep(${READDIR_SLEEP_SECS})
	    dirents += len(list(itertools.islice(dir, slice_size)))
	print(dirents)
	END_OF_SCRIPT
	)

	echo $file_count >"$result_file"
	echo $files_iterated >>"$result_file"
}

CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	MOUNTS=3 \
	setup_local_empty_lizardfs info

master_restarting_loop 1 &
thread "${info[mount0]}/thread1" 200 "$THREAD1_FILE" &
thread "${info[mount0]}/thread2" 500 "$THREAD2_FILE" &
thread "${info[mount1]}/thread3" 200 "$THREAD3_FILE" &
thread "${info[mount2]}/thread4" 200 "$THREAD4_FILE" &
wait

for i in $(seq 1 $THREAD_COUNT); do
	result_file="THREAD${i}_FILE"
	files_expected=$(sed '1q;d' ${!result_file})
	files_iterated=$(sed '2q;d' ${!result_file})

	MESSAGE="Thread ${i} sliced readdir test"
	expect_equals $files_expected $files_iterated
done
