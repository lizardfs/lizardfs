timeout_set 3 minutes
USE_RAMDISK=YES \
	CHUNKSERVERS=3 \
	MOUNTS=1 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_MIN_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|OPERATIONS_DELAY_INIT = 0" \
	USE_RAMDISK="YES" \
	MASTER_CUSTOM_GOALS="10 ec : \$ec(2,1)"\
	setup_local_empty_lizardfs info

function dirgenerate() {
	local level=$1
	local suffix=$2
	if [ "$level" -gt 0 ]; then
		echo "data" >> file${level}
		mkdir root${level}_${suffix}
		cd root${level}_${suffix}
		dirgenerate $((level - 1)) left
		dirgenerate $((level - 1)) right
		cd ..
	fi
}

cd "${info[mount0]}"
mkdir test
lizardfs setgoal ec test
lizardfs settrashtime 0 test

cd test
dirgenerate 8 a
cd ..
lizardfs rremove test

testfile="${info[mount0]}/test"

assert_eventually " [ ! -e "$testfile" ] " "3 seconds"

# Testing removing files by multiple users
mkdir test2
lizardfs setgoal ec test2
lizardfs settrashtime 0 test2

cd test2
dirgenerate 10 a
cd ..

test0="${info[mount0]}/test2"

(lizardfs rremove "${test0}/root10_a/root9_left") &
(lizardfs rremove "${test0}/root10_a/root9_right/root8_left") &
wait
lizardfs rremove "${test0}"

assert_eventually " [ ! -e "$test0" ] " "3 seconds"
