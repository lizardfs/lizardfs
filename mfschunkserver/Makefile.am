sbin_PROGRAMS=mfschunkserver
mfschunkserver_SOURCES= \
	config.c crc.c csserv.c cstocsconn.c hddspacemgr.c lempelziv.c \
	main.c masterconn.c replicator.c sockets.c stats.c \
	config.h crc.h csserv.h cstocsconn.h datapack.h hddspacemgr.h \
	init.h lempelziv.h main.h masterconn.h replicator.h sockets.h \
	stats.h \
	../MFSCommunication.h

AM_CPPFLAGS=-I$(top_srcdir) -DAPPNAME=mfschunkserver
