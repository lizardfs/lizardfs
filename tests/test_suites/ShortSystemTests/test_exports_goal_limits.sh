CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	MOUNTS=4 \
	MOUNT_1_EXTRA_EXPORTS="mingoal=2" \
	MOUNT_2_EXTRA_EXPORTS="maxgoal=14" \
	MOUNT_3_EXTRA_EXPORTS="mingoal=10,maxgoal=12" \
	setup_local_empty_lizardfs info

# Create some directory tree to work with
mkdir -p "${info[mount0]}"/dir{1..4}/sub{1..4}

cd "${info[mount1]}"
assert_success mfssetgoal     2  dir1/sub1
assert_success mfssetgoal -r  3  dir1/sub2
assert_success mfssetgoal     4  dir1/sub3
assert_success mfssetgoal -r  2+ dir1
assert_success mfssetgoal -r  7+ dir1
assert_success mfssetgoal    13+ dir1
assert_success mfssetgoal -r 20+ dir1
assert_failure mfssetgoal     1  dir1/sub4 # Too low!
assert_failure mfssetgoal -r  1  dir1      # Too low!
assert_failure mfssetgoal -r  1- dir1      # Too low!
assert_success mfssetgoal     1+ dir1      # We can always make a goal higher if only mingoal is set
assert_success mfssetgoal -r  1+ dir1      # We can always make a goal higher if only mingoal is set

cd "${info[mount2]}"
assert_success mfssetgoal -r 19- dir1      # We can always make a goal lower if only maxgoal is set
assert_success mfssetgoal    18- dir1      # We can always make a goal lower if only maxgoal is set
assert_success mfssetgoal -r 13- dir1
assert_success mfssetgoal     1  dir2/sub1
assert_success mfssetgoal -r  4  dir2/sub1
assert_success mfssetgoal     9  dir2/sub1
assert_success mfssetgoal -r 13  dir2/sub1
assert_success mfssetgoal    14  dir2/sub1
assert_failure mfssetgoal -r 15  dir2/sub2 # Too high!
assert_failure mfssetgoal    17  dir2/sub3 # Too high!
assert_failure mfssetgoal    20  dir2/sub4 # Too high!
assert_success mfssetgoal -r  9+ dir2
assert_success mfssetgoal -r 13+ dir2
assert_success mfssetgoal    14+ dir2
assert_failure mfssetgoal -r 15+ dir2      # Too high!

cd "${info[mount3]}"
assert_failure mfssetgoal     4  dir3/sub1 # Too low!
assert_failure mfssetgoal -r  9  dir3/sub1 # Too low!
assert_success mfssetgoal    10  dir3/sub1
assert_success mfssetgoal -r 11  dir3/sub1
assert_success mfssetgoal    12  dir3/sub1
assert_failure mfssetgoal -r 13  dir3/sub1 # Too high!
assert_failure mfssetgoal    16  dir3/sub1 # Too high!
assert_success mfssetgoal -r 12  dir3/sub1
assert_success mfssetgoal    10- dir3
assert_failure mfssetgoal -r  9- dir3      # Too low!
assert_success mfssetgoal -r 12+ dir3
assert_failure mfssetgoal    13+ dir3      # Too high!
assert_failure mfssetgoal -r 14+ dir3      # Too high!
assert_failure mfssetgoal    20+ dir3      # Too high!
