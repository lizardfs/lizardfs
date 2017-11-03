CHUNKSERVERS=4 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

pseudorandom_init

cd "${info[mount0]}"
mkdir dir
lizardfs setgoal ec22 dir
cd dir

for i in {0..19} ; do
	filesize=$( pseudorandom 8 $((6 * LIZARDFS_BLOCK_SIZE)) )
	head -c $filesize </dev/urandom >file${i}_$filesize
done

lizardfs_chunkserver_daemon 0 stop
lizardfs_chunkserver_daemon 1 stop

for file in * ; do
	MESSAGE="Overwriting $file" expect_success file-overwrite $file
	MESSAGE="Validating overwritten file" expect_success file-validate $file
done

lizardfs_chunkserver_daemon 0 start
lizardfs_chunkserver_daemon 1 start

lizardfs_wait_for_all_ready_chunkservers

for file in * ; do
	MESSAGE="Validating $file after restart" expect_success file-validate $file
done
