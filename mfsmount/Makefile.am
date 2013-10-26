bin_PROGRAMS=mfsmount

mfsmount_LDADD=$(PTHREAD_LIBS) $(FUSE_LIBS)
mfsmount_CPPFLAGS=$(PTHREAD_CPPFLAGS) -D_USE_PTHREADS $(FUSE_CFLAGS) -DFUSE_USE_VERSION=26 -DAPPNAME=mfsmount

mfsmount_SOURCES=\
	block_xor.cc block_xor.h \
	chunk_connector.cc chunk_connector.h \
	chunk_locator.cc chunk_locator.h \
	chunk_reader.cc chunk_reader.h \
	chunkloccache.cc chunkloccache.h \
	chunkserver_stats.cc chunkserver_stats.h \
	dirattrcache.cc dirattrcache.h \
	mastercomm.cc mastercomm.h \
	masterproxy.cc masterproxy.h \
	mfs_fuse.cc mfs_fuse.h \
	mfs_meta_fuse.cc mfs_meta_fuse.h \
	mount_config.cc mount_config.h \
	oplog.cc oplog.h \
	read_operation_executor.cc read_operation_executor.h \
	read_operation_planner.cc read_operation_planner.h \
	read_plan_executor.cc read_plan_executor.h \
	readdata.cc readdata.h \
	stats.cc stats.h \
	symlinkcache.cc symlinkcache.h \
	writedata.cc writedata.h \
	main.cc \
	../mfscommon/chunk_type.cc ../mfscommon/chunk_type.h \
	../mfscommon/connection_pool.cc ../mfscommon/connection_pool.h \
	../mfscommon/crc.cc ../mfscommon/crc.h \
	../mfscommon/datapack.h ../mfscommon/massert.h \
	../mfscommon/goal.cc ../mfscommon/goal.h \
	../mfscommon/md5.cc ../mfscommon/md5.h \
	../mfscommon/MFSCommunication.h \
	../mfscommon/mfsstrerr.h \
	../mfscommon/pcqueue.cc ../mfscommon/pcqueue.h \
	../mfscommon/sockets.cc ../mfscommon/sockets.h \
	../mfscommon/strerr.cc ../mfscommon/strerr.h \
	../mfscommon/time_utils.cc ../mfscommon/time_utils.h
