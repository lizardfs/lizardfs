bin_PROGRAMS=mfsmount

mfsmount_LDADD=$(PTHREAD_LIBS) $(FUSE_LIBS)
mfsmount_CPPFLAGS=$(PTHREAD_CPPFLAGS) -D_USE_PTHREADS $(FUSE_CFLAGS) -DFUSE_USE_VERSION=26 -DAPPNAME=mfsmount

mfsmount_SOURCES=\
	block_xor.cc block_xor.h \
	chunk_connector.cc chunk_connector.h \
	chunk_locator.cc chunk_locator.h \
	chunk_reader.cc chunk_reader.h \
	read_operation_executor.cc read_operation_executor.h \
	read_operation_planner.cc read_operation_planner.h \
	read_plan_executor.cc read_plan_executor.h \
	dirattrcache.cc dirattrcache.h \
	symlinkcache.cc symlinkcache.h \
	chunkloccache.cc chunkloccache.h \
	mfs_fuse.cc mfs_fuse.h \
	mfs_meta_fuse.cc mfs_meta_fuse.h \
	mastercomm.cc mastercomm.h \
	masterproxy.cc masterproxy.h \
	readdata.cc readdata.h \
	writedata.cc writedata.h \
	csdb.cc csdb.h \
	stats.cc stats.h \
	oplog.cc oplog.h \
	main.cc \
	../mfscommon/chunk_type.cc ../mfscommon/chunk_type.h \
	../mfscommon/pcqueue.cc ../mfscommon/pcqueue.h \
	../mfscommon/connection_pool.cc ../mfscommon/connection_pool.h \
	../mfscommon/crc.cc ../mfscommon/crc.h \
	../mfscommon/md5.cc ../mfscommon/md5.h \
	../mfscommon/sockets.cc ../mfscommon/sockets.h \
	../mfscommon/strerr.cc ../mfscommon/strerr.h \
	../mfscommon/datapack.h ../mfscommon/massert.h \
	../mfscommon/goal.cc ../mfscommon/goal.h \
	../mfscommon/mfsstrerr.h \
	../mfscommon/MFSCommunication.h
