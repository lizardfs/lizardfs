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
	sudo umount $TEMP_DIR/mnt/nfs3
	sudo umount $TEMP_DIR/mnt/nfs4
	sudo umount $TEMP_DIR/mnt/nfs41
	sudo pkill -9 ganesha.nfsd
}

cd ${info[mount0]}

mkdir $TEMP_DIR/mnt/nfs3
mkdir $TEMP_DIR/mnt/nfs4
mkdir $TEMP_DIR/mnt/nfs41
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

cd ${info[mount0]}

cat <<EOF > ${info[mount0]}/etc/ganesha/ganesha.conf
EXPORT
{
	Attr_Expiration_Time = 0;
	Export_Id = 77;
	Path = /;
	Pseudo = /;
	Access_Type = RW;
	FSAL {
		Name = LizardFS;
		hostname = localhost;
		port = ${lizardfs_info_[matocl]};
	}
	Protocols = 3, 4;
}
LizardFS {
	PNFS_DS = true;
	PNFS_MDS = true;
}
NFSV4 {
	Grace_Period = 5;
}
EOF

sudo ${info[mount0]}/bin/ganesha.nfsd -f ${info[mount0]}/etc/ganesha/ganesha.conf
sudo mount -o nfsvers=4 localhost:/ $TEMP_DIR/mnt/nfs4
sudo mount -o v4.1 localhost:/ $TEMP_DIR/mnt/nfs41
sudo mount -o nfsvers=3 localhost:/ $TEMP_DIR/mnt/nfs3

git clone git://git.linux-nfs.org/projects/steved/cthon04.git
cd cthon04
CC="ccache gcc" make all

export NFSTESTDIR=$TEMP_DIR/mnt/nfs3/cthon_test

./runtests -b -n
# ./runtests -l -n # locks do not work in ganesha with nfs v3
# ./runtests -s -n # locks do not work in ganesha with nfs v3

export NFSTESTDIR=$TEMP_DIR/mnt/nfs4/cthon_test

./runtests -b -n
./runtests -l -n
./runtests -s -n

export NFSTESTDIR=$TEMP_DIR/mnt/nfs41/cthon_test

./runtests -b -n
./runtests -l -n
./runtests -s -n

test_error_cleanup || true
