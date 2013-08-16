sbin_PROGRAMS=mfsmetalogger

AM_CPPFLAGS=-I$(top_srcdir)/mfscommon -DAPPNAME=mfsmetalogger
AM_LDFLAGS=$(ZLIB_LIBS)

mfsmetalogger_SOURCES= \
	masterconn.cc masterconn.h \
	init.h \
	../mfscommon/main.cc ../mfscommon/main.h \
	../mfscommon/cfg.cc ../mfscommon/cfg.h \
	../mfscommon/crc.cc ../mfscommon/crc.h \
	../mfscommon/event.cc ../mfscommon/event.h \
	../mfscommon/sockets.cc ../mfscommon/sockets.h \
	../mfscommon/strerr.cc ../mfscommon/strerr.h \
	../mfscommon/datapack.h ../mfscommon/massert.h ../mfscommon/slogger.h \
	../mfscommon/MFSCommunication.h
