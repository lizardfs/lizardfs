#
# To run this test you need to install libntirpc-1.5 and add following
# line to /etc/sudoers.d/lizardfstest
#
# lizardfstest ALL = NOPASSWD: ALL
#
#

timeout_set 5 minutes

CHUNKSERVERS=5 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_EXTRA_CONFIG="READ_AHEAD_KB = 1024|MAX_READ_BEHIND_KB = 2048"
	setup_local_empty_lizardfs info

MINIMUM_PARALLEL_JOBS=4
MAXIMUM_PARALLEL_JOBS=16
PARALLEL_JOBS=$(get_nproc_clamped_between ${MINIMUM_PARALLEL_JOBS} ${MAXIMUM_PARALLEL_JOBS})

test_error_cleanup() {
	for x in 1 2 97 99; do
		sudo umount $TEMP_DIR/mnt/nfs$x
	done
	sudo pkill -9 ganesha.nfsd
}

cd ${info[mount0]}

mkdir $TEMP_DIR/mnt/nfs{1,2,97,99}
mkdir ganesha

cp -R "$SOURCE_DIR"/external/nfs-ganesha-2.5-stable nfs-ganesha-2.5-stable
cp -R "$SOURCE_DIR"/external/ntirpc-1.5 ntirpc-1.5

rm -R nfs-ganesha-2.5-stable/src/libntirpc
ln -s ../../ntirpc-1.5 nfs-ganesha-2.5-stable/src/libntirpc

mkdir nfs-ganesha-2.5-stable/src/build
cd nfs-ganesha-2.5-stable/src/build
CC="ccache gcc" cmake -DCMAKE_INSTALL_PREFIX=${info[mount0]} ..
make -j${PARALLEL_JOBS} install
cp ${LIZARDFS_ROOT}/lib/ganesha/libfsallizardfs* ${info[mount0]}/lib/ganesha

# mkdir ${info[mount0]}/ntirpc-1.5/build
# cd ${info[mount0]}/ntirpc-1.5/build
# CC="ccache gcc" cmake -DCMAKE_INSTALL_PREFIX=${info[mount0]} ..
# make -j${PARALLEL_JOBS} install


cat <<EOF > ${info[mount0]}/etc/ganesha/ganesha.conf
EXPORT
{
	Attr_Expiration_Time = 0;
	Export_Id = 1;
	Path = /export1;
	Pseudo = /e1;
	Access_Type = RW;
	FSAL {
		Name = LizardFS;
		hostname = localhost;
		port = ${lizardfs_info_[matocl]};
	}
	Protocols = 3, 4;
}
EXPORT
{
	Attr_Expiration_Time = 0;
	Export_Id = 2;
	Path = /export2;
	Pseudo = /e2;
	Access_Type = RW;
	FSAL {
		Name = LizardFS;
		hostname = localhost;
		port = ${lizardfs_info_[matocl]};
	}
	Protocols = 3, 4;
}
EXPORT
{
	Attr_Expiration_Time = 0;
	Export_Id = 97;
	Path = /;
	Pseudo = /e97;
	Access_Type = MDONLY;
	FSAL {
		Name = LizardFS;
		hostname = localhost;
		port = ${lizardfs_info_[matocl]};
	}
	Protocols = 4;
}
EXPORT
{
	Attr_Expiration_Time = 0;
	Export_Id = 99;
	Path = /;
	Pseudo = /e99;
	Access_Type = RO;
	FSAL {
		Name = LizardFS;
		hostname = localhost;
		port = ${lizardfs_info_[matocl]};
	}
	Protocols = 4;
}
LizardFS {
	PNFS_DS = true;
	PNFS_MDS = true;
}
NFSV4 {
	Grace_Period = 5;
}
EOF

mkdir ${info[mount0]}/export{1,2}

touch ${info[mount0]}/export1/test1
touch ${info[mount0]}/export2/test2

sudo ${info[mount0]}/bin/ganesha.nfsd -f ${info[mount0]}/etc/ganesha/ganesha.conf

assert_eventually 'showmount -e localhost'

for x in 1 2 99; do
	sudo mount -o v4.1 localhost:/e$x $TEMP_DIR/mnt/nfs$x
done
sudo mount -o nfsvers=4 localhost:/e97 $TEMP_DIR/mnt/nfs97

find $TEMP_DIR/mnt/nfs1 * | grep test1
assert_empty "$(find $TEMP_DIR/mnt/nfs1 | grep test2 | cat)"
find $TEMP_DIR/mnt/nfs2 * | grep test2
assert_empty "$(find $TEMP_DIR/mnt/nfs2 | grep test1 | cat)"

ls -l $TEMP_DIR/mnt/nfs1
ls -l $TEMP_DIR/mnt/nfs2
ls -l $TEMP_DIR/mnt/nfs97
ls -l $TEMP_DIR/mnt/nfs99

FILE_SIZE=1234567 file-generate $TEMP_DIR/mnt/nfs1/test1.bin
FILE_SIZE=2345678 file-generate $TEMP_DIR/mnt/nfs2/test2.bin

file-validate $TEMP_DIR/mnt/nfs1/test1.bin
file-validate $TEMP_DIR/mnt/nfs2/test2.bin
file-validate $TEMP_DIR/mnt/nfs99/export1/test1.bin
file-validate $TEMP_DIR/mnt/nfs99/export2/test2.bin
# Files on export97 are "metadata only", so file validation should fail
assert_failure file-validate $TEMP_DIR/mnt/nfs97/export1/test1.bin
assert_failure file-validate $TEMP_DIR/mnt/nfs97/export2/test2.bin

test_error_cleanup
