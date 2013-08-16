sbin_PROGRAMS=mfsmaster

AM_CPPFLAGS=-I$(top_srcdir)/mfscommon -DAPPNAME=mfsmaster
AM_LDFLAGS=$(ZLIB_LIBS)

mfsmaster_SOURCES=\
	itree.cc itree.h \
	topology.cc topology.h \
	exports.cc exports.h \
	changelog.cc changelog.h \
	chunks.cc chunks.h \
	filesystem.cc filesystem.h \
	matocsserv.cc matocsserv.h \
	matoclserv.cc matoclserv.h \
	matomlserv.cc matomlserv.h \
	masterconn.cc masterconn.h \
	datacachemgr.cc datacachemgr.h \
	chartsdata.cc chartsdata.h \
	init.h \
	../mfscommon/main.cc ../mfscommon/main.h \
	../mfscommon/cfg.cc ../mfscommon/cfg.h \
	../mfscommon/random.cc ../mfscommon/random.h \
	../mfscommon/md5.cc ../mfscommon/md5.h \
	../mfscommon/crc.cc ../mfscommon/crc.h \
	../mfscommon/event.cc ../mfscommon/event.h \
	../mfscommon/sockets.cc ../mfscommon/sockets.h \
	../mfscommon/charts.cc ../mfscommon/charts.h \
	../mfscommon/strerr.cc ../mfscommon/strerr.h \
	../mfsmetarestore/restore.cc ../mfsmetarestore/restore.h \
	../mfsmetarestore/merger.cc ../mfsmetarestore/merger.h \
	../mfscommon/datapack.h ../mfscommon/massert.h \
	../mfscommon/slogger.h ../mfscommon/mfsstrerr.h \
	../mfscommon/hashfn.h \
	../mfscommon/MFSCommunication.h
