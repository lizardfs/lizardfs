sbin_PROGRAMS=mfschunkserver

AM_CPPFLAGS=-std=c99 -I$(top_srcdir)/mfscommon -DMFSMAXFILES=10000 -D_USE_PTHREADS $(PTHREAD_CPPFLAGS) -DAPPNAME=mfschunkserver
AM_LDFLAGS=$(PTHREAD_LIBS) $(ZLIB_LIBS)

mfschunkserver_SOURCES= \
	bgjobs.c bgjobs.h \
	csserv.c csserv.h \
	hddspacemgr.c hddspacemgr.h \
	masterconn.c masterconn.h \
	replicator.c replicator.h \
	chartsdata.c chartsdata.h \
	init.h \
	../mfscommon/main.c ../mfscommon/main.h \
	../mfscommon/cfg.c ../mfscommon/cfg.h \
	../mfscommon/th_sem.c ../mfscommon/th_sem.h \
	../mfscommon/th_queue.c ../mfscommon/th_queue.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/charts.c ../mfscommon/charts.h \
	../mfscommon/datapack.h \
	../mfscommon/MFSCommunication.h

mfschunkserver_CFLAGS=$(PTHREAD_CFLAGS)
