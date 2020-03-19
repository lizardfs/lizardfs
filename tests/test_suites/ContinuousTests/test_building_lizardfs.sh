timeout_set 60 minutes
continuous_test_begin
lizardfs setgoal 2 .
workspace=$(pwd)

MINIMUM_PARALLEL_JOBS=4
MAXIMUM_PARALLEL_JOBS=16
PARALLEL_JOBS=$(get_nproc_clamped_between ${MINIMUM_PARALLEL_JOBS} ${MAXIMUM_PARALLEL_JOBS})

# Prepare a reasonably up-to-date copy of LizardFS sources from GitHub
if [[ ! -d lizardfs ]]; then
	assert_success git clone "https://github.com/lizardfs/lizardfs.git" lizardfs
fi
# We will use mtime of the lizardfs/ directory to measure time since the last update
if [[ $(stat --format=%Y lizardfs) -lt $(date +%s -d "8 hours ago") ]]; then
	cd lizardfs
	assert_success git pull
	assert_success touch .  # Update timestamp checked by this 'if'
fi

# Update 5 out of 1000 randomly chosen copies of our repository
N=1000
for i in {0..4}; do
	cd "$workspace"
	# This makes copies with low IDs more frequently used
	ranges=(1 10 $N $N $N $N)
	subdir="copy_$((RANDOM % ranges[i]))"
	MESSAGE="testing directory $workspace/$subdir"
	if [[ ! -d "$subdir" ]]; then
		# If there is no such copy yet -- create a new one
		assert_success git clone "$workspace/lizardfs" "$subdir"
	fi
	assert_success lizardfs setgoal -r "$(random 2 3)" "$subdir"  # Change goal to 2 or 3 (randomly)
	cd "$subdir"
	assert_success git reset --hard HEAD^  # Make sure that 'git pull' will change something
	assert_success git pull
	assert_success mkdir -p build
	cd build
	assert_success cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=../install_prefix
	assert_success make -j${PARALLEL_JOBS}
	assert_success make install
done
