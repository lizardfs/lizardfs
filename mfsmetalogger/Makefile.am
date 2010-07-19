sbin_PROGRAMS=mfsmetalogger

AM_CPPFLAGS=-std=c99 -I$(top_srcdir)/mfscommon -DAPPNAME=mfsmetalogger
AM_LDFLAGS=$(ZLIB_LIBS)

mfsmetalogger_SOURCES= \
	masterconn.c masterconn.h \
	init.h \
	../mfscommon/main.c ../mfscommon/main.h \
	../mfscommon/cfg.c ../mfscommon/cfg.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/datapack.h \
	../mfscommon/MFSCommunication.h
