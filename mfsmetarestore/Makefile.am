sbin_PROGRAMS=mfsmetarestore

AM_CPPFLAGS=-I$(top_srcdir)/mfsmaster -I$(top_srcdir)/mfscommon -DAPPNAME=mfsmetarestore -DMETARESTORE

mfsmetarestore_SOURCES=\
	main.c \
	restore.c restore.h \
	../mfsmaster/filesystem.c ../mfsmaster/filesystem.h \
	../mfsmaster/chunks.c ../mfsmaster/chunks.h \
	../mfscommon/datapack.h \
	../mfscommon/MFSCommunication.h
