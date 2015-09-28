timeout_set 1 minutes

USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="enablefilelocks=1" \
	setup_local_empty_lizardfs info


# Create files
cd "${info[mount0]}"
mkdir "${info[mount0]}/dir"
for size in {1,2,4}M; do
	FILE_SIZE="$size" assert_success file-generate "${info[mount0]}/dir/file_$size"
done


opcount=0

function assert_operation_performed() {
	opcount=$((opcount + 1))
	assert_eventually_prints "$1" "sed -n ${opcount}p posixlock.log"
}

function assert_operation_not_performed() {
	assert_eventually_prints "" "sed -n ${1}p posixlock.log"
}

function readlock() {
	posixlockcmd $1 r $2 $3 >> posixlock.log &
	assert_operation_performed "read  open:   $1" posixlock
}

function writelock() {
	posixlockcmd $1 w $2 $3 >> posixlock.log &
	assert_operation_performed "write open:   $1" posixlock
}

function unlock() {
	kill -s SIGUSR1 $1
}

# Place read lock on first file
readlock "dir/file_1M" 0 100
readlocks[1]=$!
assert_operation_performed "read  lock:   dir/file_1M"

readlock "dir/file_1M" 0 100
readlocks[2]=$!

assert_operation_performed "read  lock:   dir/file_1M"

# Try to place write lock on first file
writelock "dir/file_1M" 200 300
writelocks[1]=$!
assert_operation_performed "write lock:   dir/file_1M"

unlock ${readlocks[1]}
assert_operation_performed "read  unlock: dir/file_1M"

writelock "dir/file_1M" 50 150
writelocks[2]=$!

unlock ${readlocks[2]}
assert_operation_performed "read  unlock: dir/file_1M"

assert_operation_performed "write lock:   dir/file_1M"

writelock "dir/file_1M" 0 0
writelocks[3]=$!

assert_operation_not_performed $((opcount+1))

unlock ${writelocks[1]}
unlock ${writelocks[2]}

master_port=${info[master0_matocl]}

assert_operation_performed "write unlock: dir/file_1M"
