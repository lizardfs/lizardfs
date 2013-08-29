bin_PROGRAMS=mfsmount

mfsmount_LDADD=$(PTHREAD_LIBS) $(FUSE_LIBS)
mfsmount_CPPFLAGS=$(PTHREAD_CPPFLAGS) -D_USE_PTHREADS $(FUSE_CFLAGS) -DFUSE_USE_VERSION=26 -DAPPNAME=mfsmount

mfsmount_SOURCES=\
	dirattrcache.cc dirattrcache.h \
	symlinkcache.cc symlinkcache.h \
	chunkloccache.cc chunkloccache.h \
	mfs_fuse.cc mfs_fuse.h \
	mfs_meta_fuse.cc mfs_meta_fuse.h \
	mastercomm.cc mastercomm.h \
	masterproxy.cc masterproxy.h \
	cscomm.cc cscomm.h \
	readdata.cc readdata.h \
	writedata.cc writedata.h \
	csdb.cc csdb.h \
	stats.cc stats.h \
	oplog.cc oplog.h \
	main.cc \
	../mfscommon/pcqueue.cc ../mfscommon/pcqueue.h \
	../mfscommon/connection_pool.cc ../mfscommon/connection_pool.h \
	../mfscommon/crc.cc ../mfscommon/crc.h \
	../mfscommon/md5.cc ../mfscommon/md5.h \
	../mfscommon/sockets.cc ../mfscommon/sockets.h \
	../mfscommon/strerr.cc ../mfscommon/strerr.h \
	../mfscommon/datapack.h ../mfscommon/massert.h \
	../mfscommon/mfsstrerr.h \
	../mfscommon/MFSCommunication.h
