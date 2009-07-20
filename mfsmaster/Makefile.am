sbin_PROGRAMS=mfsmaster

AM_CPPFLAGS=-std=c99 -I$(top_srcdir)/mfscommon -D_GNU_SOURCE -DAPPNAME=mfsmaster
AM_LDFLAGS=$(ZLIB_LIBS)

mfsmaster_SOURCES=\
	acl.h acl.c \
	changelog.c changelog.h \
	chunks.c chunks.h \
	filesystem.c filesystem.h \
	matocsserv.c matocsserv.h \
	matocuserv.c matocuserv.h \
	random.c random.h \
	stats.c stats.h \
	init.h \
	../mfscommon/main.c ../mfscommon/main.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/datapack.h \
	../mfscommon/MFSCommunication.h

mfsmaster_LDADD=../mfscommon/libmfscommon.a
