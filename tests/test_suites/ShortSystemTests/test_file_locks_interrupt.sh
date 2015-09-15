timeout_set 1 minutes

USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="enablefilelocks=1" \
	MOUNTS=2 \
	setup_local_empty_lizardfs info

mountpoint0="${info[mount0]}"
mountpoint1="${info[mount1]}"
# how many lock should be removed
rmpcount=0

cd "${info[mount0]}"
mkdir "$mountpoint0/intr"

function test_interrupt() {
	lock_type="$1"
	export MESSAGE="Testing $lock_type lock"
	lockcmd="${lock_type}cmd"
	((rmpcount=rmpcount+1))

	lockfile0="$mountpoint0/intr/$lock_type"
	lockfile1="$mountpoint1/intr/$lock_type"
	touch $lockfile0

	# lock 0
	$lockcmd $lockfile0 w >> lock0.log &
	pidlock0=$!

	# wait for lockcmd to set a lock
	assert_eventually_prints "write lock:   $lockfile0" 'tail -1 lock0.log'

	# lock 1 - this one should sleep
	$lockcmd $lockfile1 w 2>> lock1_err.log &
	pidlock1=$!

	# wait for lockcmd to sleep on a lock
	sleep $(timeout_rescale_seconds 1)

	# send a signal
	kill -s SIGUSR2 "$pidlock1"

	# make sure flock was interrupted and lock was removed
	assert_eventually_matches '.*failed: Interrupted system call' 'tail -1 lock1_err.log'
	assert_eventually_prints $rmpcount "get_changes ${info[master0_data_path]} | grep RMPLOCK | wc -l"

	# cleanup
	rm *.log
}

test_interrupt flock
test_interrupt posixlock