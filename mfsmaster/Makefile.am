sbin_PROGRAMS=mfsmaster

AM_CPPFLAGS=-I$(top_srcdir)/mfscommon -DAPPNAME=mfsmaster
AM_LDFLAGS=$(ZLIB_LIBS)

mfsmaster_SOURCES=\
	itree.h itree.c \
	topology.h topology.c \
	exports.h exports.c \
	changelog.c changelog.h \
	chunks.c chunks.h \
	filesystem.c filesystem.h \
	matocsserv.c matocsserv.h \
	matoclserv.c matoclserv.h \
	matomlserv.c matomlserv.h \
	datacachemgr.c datacachemgr.h \
	chartsdata.c chartsdata.h \
	init.h \
	../mfscommon/main.c ../mfscommon/main.h \
	../mfscommon/cfg.c ../mfscommon/cfg.h \
	../mfscommon/random.c ../mfscommon/random.h \
	../mfscommon/md5.c ../mfscommon/md5.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/charts.c ../mfscommon/charts.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/datapack.h ../mfscommon/massert.h \
	../mfscommon/slogger.h ../mfscommon/mfsstrerr.h \
	../mfscommon/MFSCommunication.h
