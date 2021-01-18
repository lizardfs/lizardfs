CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	MOUNTS=0 \
	setup_local_empty_lizardfs info

assert_success c-client-example ${info[matocl]}
