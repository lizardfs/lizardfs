timeout_set 1 minutes

USE_RAMDISK=YES \
	MOUNTS=5
	MOUNT_EXTRA_CONFIG="enablefilelocks=1" \
	setup_local_empty_lizardfs info

touch ${info[mount0]}/lockfile

# Launch ping pong instances
for i in {0..4}; do
	cd ${info[mount$i]}
	lzfs_ping_pong lockfile 6&
	ping_pongs[$i]=$!
done

# Ensure that all ping pong tests finished with no errors
for i in {0..4}; do
	wait ${ping_pongs[$i]}
	assert_equals 0 $?
done
