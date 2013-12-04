timeout_set 1 hour
assert_program_installed git
assert_program_installed cmake
assert_program_installed rsync

test_worker() {
	cd "$1"
	git clone https://github.com/lizardfs/lizardfs.git
	mkdir lizardfs/build
	cd lizardfs/build
	cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=../install
	make -j5 install
	cd ../..
	rsync -a lizardfs/ copy_lizardfs
	find lizardfs -type f | while read file; do
		MESSAGE="Data after rsync is corrupted" expect_files_equal "$file" "copy_$file"
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
wait %1
wait %2
wait %3
