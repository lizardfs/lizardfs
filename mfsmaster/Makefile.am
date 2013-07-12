sbin_PROGRAMS=mfsmaster

AM_CPPFLAGS=-DAPPNAME=mfsmaster
AM_LDFLAGS=$(ZLIB_LIBS)

mfsmaster_SOURCES=\
	itree.h itree.cc \
	topology.h topology.cc \
	exports.h exports.cc \
	changelog.cc changelog.h \
	chunks.cc chunks.h \
	filesystem.cc filesystem.h \
	matocsserv.cc matocsserv.h \
	matoclserv.cc matoclserv.h \
	matomlserv.cc matomlserv.h \
	datacachemgr.cc datacachemgr.h \
	chartsdata.cc chartsdata.h \
	init.h \
	../mfsdaemonmain/main.cc ../mfsdaemonmain/main.h \
	../mfscommon/cfg.cc ../mfscommon/cfg.h \
	../mfscommon/random.cc ../mfscommon/random.h \
	../mfscommon/md5.cc ../mfscommon/md5.h \
	../mfscommon/crc.cc ../mfscommon/crc.h \
	../mfscommon/sockets.cc ../mfscommon/sockets.h \
	../mfscommon/charts.cc ../mfscommon/charts.h \
	../mfscommon/strerr.cc ../mfscommon/strerr.h \
	../mfscommon/datapack.h ../mfscommon/massert.h \
	../mfscommon/slogger.h ../mfscommon/mfsstrerr.h \
	../mfscommon/hashfn.h \
	../mfscommon/MFSCommunication.h
