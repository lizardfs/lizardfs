sbin_PROGRAMS=mfsmetarestore

AM_CPPFLAGS=-I$(top_srcdir)/mfsmaster -I$(top_srcdir) -DAPPNAME=mfsmetarestore -DMETARESTORE

mfsmetarestore_SOURCES=\
	main.c restore.c \
	../mfsmaster/filesystem.c ../mfsmaster/chunks.c \
	datapack.h restore.h \
	../mfsmaster/filesystem.h ../mfsmaster/chunks.h \
	../MFSCommunication.h
