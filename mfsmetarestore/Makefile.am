sbin_PROGRAMS=mfsmetarestore

AM_CPPFLAGS=-I$(top_srcdir)/mfsmaster -DAPPNAME=mfsmetarestore -DMETARESTORE

mfsmetarestore_SOURCES=\
	main.cc \
	merger.cc merger.h \
	restore.cc restore.h \
	../mfsmaster/filesystem.cc ../mfsmaster/filesystem.h \
	../mfsmaster/chunks.cc ../mfsmaster/chunks.h \
	../mfscommon/strerr.cc ../mfscommon/strerr.h \
	../mfscommon/datapack.h ../mfscommon/massert.h ../mfscommon/slogger.h \
	../mfscommon/MFSCommunication.h
