timeout_set 1 minutes

USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="enablefilelocks=1" \
	setup_local_empty_lizardfs info


	# Create files
	cd "${info[mount0]}"
	mkdir "${info[mount0]}/dir"
	for size in {1,2,4,5}M; do
		FILE_SIZE="$size" assert_success file-generate "${info[mount0]}/dir/file_$size"
	done

function test_locks() {
	local locktype=$1
	opcount=0

	function settype() {
		if [[ $# -lt 2 ]]; then
			ltype=${locktype};
		else
			ltype=$2;
		fi
	}

	function assert_operation_performed() {
		settype "$@"

		opcount=$((opcount + 1))
		assert_eventually_prints "$1" "sed -n ${opcount}p ${ltype}.log"
	}

	function readlock() {
		settype "$@"

		${ltype}cmd $1 r >> ${ltype}.log &
		assert_operation_performed "read  open:   $1" ${ltype}
	}

	function writelock() {
		settype "$@"

		${ltype}cmd $1 w >> ${ltype}.log &
		assert_operation_performed "write open:   $1" ${ltype}
	}


	function unlock() {
		kill -s SIGUSR1 $1
	}

	# Place read lock on first file
	readlock "dir/file_1M"
	readlocks[1]=$!

	assert_operation_performed "read  lock:   dir/file_1M"

	readlock "dir/file_1M"
	readlocks[2]=$!

	assert_operation_performed "read  lock:   dir/file_1M"

	# Try to place write lock on first file
	writelock "dir/file_1M"
	writelocks[1]=$!

	# Place write lock on second file
	writelock "dir/file_2M"
	writelocks[2]=$!

	assert_operation_performed "write lock:   dir/file_2M"

	# Remove first read lock
	unlock ${readlocks[1]}

	assert_operation_performed "read  unlock: dir/file_1M"

	# Remove second read lock
	# Pending write lock should be applied immediately
	unlock ${readlocks[2]}

	assert_operation_performed "read  unlock: dir/file_1M"
	assert_operation_performed "write lock:   dir/file_1M"

	# Try to place 2 read locks on second file
	readlock "dir/file_2M"
	readlocks[1]=$!
	readlock "dir/file_2M"
	readlocks[2]=$!


	# Remove write lock on second file
	# Pending read locks should be applied immediately
	unlock ${writelocks[2]}

	assert_operation_performed "write unlock: dir/file_2M"
	assert_operation_performed "read  lock:   dir/file_2M"
	assert_operation_performed "read  lock:   dir/file_2M"

	# Create write lock for file 3
	writelock "dir/file_4M"
	writelocks[1]=$!

	assert_operation_performed "write lock:   dir/file_4M"

	# Try to create lots of read locks for file 3
	for i in {1..256}; do
		readlock "dir/file_4M"
		readlocks[i]=$!
	done

	# Remove write lock for file3
	# Pending read locks should be applied immediately
	unlock ${writelocks[1]}
	assert_operation_performed "write unlock: dir/file_4M"
	for i in {1..256}; do
		assert_operation_performed "read  lock:   dir/file_4M"
	done

	# Remove all read locks for file3
	for i in {1..256}; do
		unlock ${readlocks[i]}
		assert_operation_performed "read  unlock: dir/file_4M"
	done

	# Try to create another write lock for file 3 and remove it immediately
	writelock "dir/file_4M"
	writelocks[1]=$!
	unlock ${writelocks[1]}

	assert_operation_performed "write lock:   dir/file_4M"
	assert_operation_performed "write unlock: dir/file_4M"

	# Test if closing file descriptor removes the lock
	writelock "dir/file_5M"
	writelocks[1]=$!

	assert_operation_performed "write lock:   dir/file_5M"
	kill -s SIGINT ${writelocks[1]}

	assert_operation_performed "write closed: dir/file_5M"

	# If we can acquire lock then close removed write lock
	readlock "dir/file_5M"
	readlocks[1]=$!

	assert_operation_performed "read  lock:   dir/file_5M"

	unlock ${readlocks[1]}
	assert_operation_performed "read  unlock: dir/file_5M"
}

test_locks posixlock
test_locks flock
