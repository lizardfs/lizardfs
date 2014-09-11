timeout_set '40 minutes'

metaservers_nr=2
MASTERSERVERS=$metaservers_nr \
	CHUNKSERVERS=2 \
	CHUNKSERVER_EXTRA_CONFIG="MASTER_RECONNECTION_DELAY = 1" \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota" \
	MOUNT_EXTRA_CONFIG="mfsacl,mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="MAGIC_AUTO_FILE_REPAIR = 1" \
	setup_local_empty_lizardfs info

assert_program_installed git
assert_program_installed cmake

master_kill_loop() {
	# Start shadow masters
	for ((shadow_id=1 ; shadow_id<metaservers_nr; ++shadow_id)); do
		lizardfs_master_n $shadow_id start
		assert_eventually "lizardfs_shadow_synchronized $shadow_id"
	done

	loop_nr=0
	# Let the master run for few seconds and then replace it with another one
	while true; do
		echo "Loop nr $loop_nr"
		sleep 5

		prev_master_id=$((loop_nr % metaservers_nr))
		new_master_id=$(((loop_nr + 1) % metaservers_nr))
		loop_nr=$((loop_nr + 1))

		# Kill the previous master
		assert_eventually "lizardfs_shadow_synchronized $new_master_id"
		lizardfs_master_daemon kill
		lizardfs_make_conf_for_shadow $prev_master_id

		# Promote a next master
		lizardfs_make_conf_for_master $new_master_id
		lizardfs_master_daemon reload

		# Demote previous master to shadow
		lizardfs_make_conf_for_shadow $prev_master_id
		lizardfs_master_n $prev_master_id start
		assert_eventually "lizardfs_shadow_synchronized $prev_master_id"

		lizardfs_wait_for_all_ready_chunkservers
	done
}

# Daemonize the master kill loop
master_kill_loop &

cd "${info[mount0]}"
assert_success git clone https://github.com/lizardfs/lizardfs.git
mfssetgoal -r 2 lizardfs
mkdir lizardfs/build
cd lizardfs/build
assert_success cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=../install
assert_success make -j5 install
