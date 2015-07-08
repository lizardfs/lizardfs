timeout_set 1 minute

percent=63
cache=149 # megabytes
CHUNKSERVERS=1 \
	MOUNT_EXTRA_CONFIG="mfscacheperinodepercentage=$percent | mfswritecachesize=$cache" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

cs0_pid="$(lizardfs_chunkserver_daemon 0 test 2>&1 | sed 's/.*pid: //')"
test -n $cs0_pid
kill -s SIGSTOP $cs0_pid

file_created_on_success=$TEMP_DIR/success
testing_thread() {
	# We use a single descriptor for both dd's below, since every dd ends with
	# close, thus calls flush, thus would hang
	{
		that_much_should_fit_in_cache=$(((cache - 1) * percent / 100))
		head -c ${that_much_should_fit_in_cache}M /dev/zero
		touch "$file_created_on_success" # Hurray, we managed to use cache
		head -c 3M /dev/zero # This is expected to hang forever
		test_add_failure "We shouldn't ever get here"
	} | stdbuf -i0 -o0 tee "${info[mount0]}/some_file" > /dev/null
}

# The code executed by a following command is supposed to hang, thus we run it in a background:
testing_thread &
assert_success wait_for 'test -a "$file_created_on_success"' '15 seconds'
sleep 5 # let's let the second 'dd' run and (possibly) fail the test

# Kill background processes before exit to avoid false negatives from valgrind
if [[ ${USE_VALGRIND} ]]; then
	jobs -p | xargs kill -KILL
	assert_eventually_prints "" "jobs -p"
fi
