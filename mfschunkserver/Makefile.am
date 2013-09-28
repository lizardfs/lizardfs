sbin_PROGRAMS=mfschunkserver

AM_CPPFLAGS=-DMFSMAXFILES=10000 -D_USE_PTHREADS $(PTHREAD_CPPFLAGS) -DAPPNAME=mfschunkserver
AM_LDFLAGS=$(PTHREAD_LIBS) $(ZLIB_LIBS)

mfschunkserver_SOURCES= \
	output_buffers.cc output_buffers.h \
	bgjobs.cc bgjobs.h \
	chunk.cc chunk.h \
	chunk_signature.cc chunk_signature.h \
	csserv.cc csserv.h \
	hddspacemgr.cc hddspacemgr.h \
	masterconn.cc masterconn.h \
	replicator.cc replicator.h \
	chartsdata.cc chartsdata.h \
	init.h \
	chunk_filename_parser.h chunk_filename_parser.cc \
	../mfsdaemonmain/main.cc ../mfsdaemonmain/main.h \
	../mfscommon/cfg.cc ../mfscommon/cfg.h \
	../mfscommon/random.cc ../mfscommon/random.h \
	../mfscommon/pcqueue.cc ../mfscommon/pcqueue.h \
	../mfscommon/crc.cc ../mfscommon/crc.h \
	../mfscommon/sockets.cc ../mfscommon/sockets.h \
	../mfscommon/charts.cc ../mfscommon/charts.h \
	../mfscommon/strerr.cc ../mfscommon/strerr.h \
	../mfscommon/datapack.h ../mfscommon/massert.h \
	../mfscommon/slogger.h ../mfscommon/mfsstrerr.h \
	../mfscommon/MFSCommunication.h\
	../mfscommon/parser.h ../mfscommon/parser.cc \
	../mfscommon/chunk_type.h ../mfscommon/chunk_type.cc \
	../mfscommon/goal.h ../mfscommon/goal.cc
