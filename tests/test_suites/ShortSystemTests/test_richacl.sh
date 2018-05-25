timeout_set 3 minutes

CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER,enablefilelocks=1" \
	CHUNKSERVER_EXTRA_CONFIG="READ_AHEAD_KB = 1024|MAX_READ_BEHIND_KB = 2048"
	setup_local_empty_lizardfs info

cd ${info[mount0]}

git clone https://github.com/andreas-gruenbacher/richacl.git

cd richacl
./autogen.sh
./configure
make check
