timeout_set 70 seconds

CHUNKSERVERS=7 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER"
	setup_local_empty_lizardfs info

cd ${info[mount0]}

mkdir dir_ec
mfssetgoal -r ec43 dir_ec
FILE_SIZE=123456789 BLOCK_SIZE=12345 file-generate dir_ec/file

lizardfs_chunkserver_daemon 0 stop
lizardfs_chunkserver_daemon 1 stop
lizardfs_chunkserver_daemon 2 stop

assert_failure "mfsfileinfo dir_ec/file | grep 'not enough parts available'"

lizardfs_chunkserver_daemon 3 stop

mfsfileinfo dir_ec/file | grep "not enough parts available"
