sbin_PROGRAMS=mfschunkserver

AM_CPPFLAGS=-std=c99 -I$(top_srcdir)/mfscommon -DMFSMAXFILES=10000 -D_GNU_SOURCE -D_THREAD_SAFE -D_REENTRANT -DAPPNAME=mfschunkserver
AM_LDFLAGS=-lpthread $(ZLIB_LIBS)

mfschunkserver_SOURCES= \
	bgjobs.c bgjobs.h \
	csserv.c csserv.h \
	hddspacemgr.c hddspacemgr.h \
	masterconn.c masterconn.h \
	replicator.c replicator.h \
	stats.c stats.h \
	th_sem.c th_sem.h \
	th_queue.c th_queue.h \
	init.h \
	../mfscommon/main.c ../mfscommon/main.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/datapack.h \
	../mfscommon/MFSCommunication.h

mfschunkserver_LDADD=../mfscommon/libmfscommon.a
