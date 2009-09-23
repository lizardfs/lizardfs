sbin_PROGRAMS=mfsmetalogger

AM_CPPFLAGS=-std=c99 -I$(top_srcdir)/mfscommon -D_GNU_SOURCE -D_THREAD_SAFE -D_REENTRANT -DAPPNAME=mfsmetalogger
AM_LDFLAGS=-lpthread $(ZLIB_LIBS)

mfsmetalogger_SOURCES= \
	masterconn.c masterconn.h \
	init.h \
	../mfscommon/main.c ../mfscommon/main.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/datapack.h \
	../mfscommon/MFSCommunication.h

mfsmetalogger_LDADD=../mfscommon/libmfscommon.a
