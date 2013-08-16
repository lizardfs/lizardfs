sbin_PROGRAMS=mfschunkserver

AM_CPPFLAGS=-I$(top_srcdir)/mfscommon -DMFSMAXFILES=10000 -D_USE_PTHREADS $(PTHREAD_CPPFLAGS) -DAPPNAME=mfschunkserver
AM_LDFLAGS=$(PTHREAD_LIBS) $(ZLIB_LIBS)

mfschunkserver_SOURCES= \
	bgjobs.cc bgjobs.h \
	csserv.cc csserv.h \
	hddspacemgr.cc hddspacemgr.h \
	masterconn.cc masterconn.h \
	replicator.cc replicator.h \
	chartsdata.cc chartsdata.h \
	init.h \
	../mfscommon/main.cc ../mfscommon/main.h \
	../mfscommon/cfg.cc ../mfscommon/cfg.h \
	../mfscommon/random.cc ../mfscommon/random.h \
	../mfscommon/pcqueue.cc ../mfscommon/pcqueue.h \
	../mfscommon/crc.cc ../mfscommon/crc.h \
	../mfscommon/event.cc ../mfscommon/event.h \
	../mfscommon/sockets.cc ../mfscommon/sockets.h \
	../mfscommon/charts.cc ../mfscommon/charts.h \
	../mfscommon/strerr.cc ../mfscommon/strerr.h \
	../mfscommon/datapack.h ../mfscommon/massert.h \
	../mfscommon/slogger.h ../mfscommon/mfsstrerr.h \
	../mfscommon/MFSCommunication.h

mfschunkserver_CFLAGS=$(PTHREAD_CFLAGS)
