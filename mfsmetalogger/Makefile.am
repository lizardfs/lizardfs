sbin_PROGRAMS=mfsmetalogger

AM_CPPFLAGS=-DAPPNAME=mfsmetalogger
AM_LDFLAGS=$(ZLIB_LIBS)

mfsmetalogger_SOURCES= \
	masterconn.cc masterconn.h \
	init.h \
	../mfsdaemonmain/main.cc ../mfsdaemonmain/main.h \
	../mfscommon/cfg.cc ../mfscommon/cfg.h \
	../mfscommon/crc.cc ../mfscommon/crc.h \
	../mfscommon/sockets.cc ../mfscommon/sockets.h \
	../mfscommon/strerr.cc ../mfscommon/strerr.h \
	../mfscommon/datapack.h ../mfscommon/massert.h ../mfscommon/slogger.h \
	../mfscommon/MFSCommunication.h
