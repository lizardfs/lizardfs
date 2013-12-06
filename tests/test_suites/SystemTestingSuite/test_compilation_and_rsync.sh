timeout_set 1 hour
assert_program_installed git
assert_program_installed cmake
assert_program_installed rsync

test_worker() {
	export MESSAGE="Testing directory $1"
	cd "$1"
	assertlocal_success git clone https://github.com/lizardfs/lizardfs.git
	mkdir lizardfs/build
	cd lizardfs/build
	assertlocal_success cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=../install
	make -j5 install
	cd ../..
	assertlocal_success rsync -a lizardfs/ copy_lizardfs
	find lizardfs -type f | while read file; do
		expect_files_equal "$file" "copy_$file"
	done
}

CHUNKSERVERS=3 \
	MOUNTS=1 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info
	
cd "${info[mount0]}"
for goal in 1 2 3; do
	mkdir "goal_$goal"
	mfssetgoal "$goal" "goal_$goal"
	test_worker "goal_$goal" &
done
wait
