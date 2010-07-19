bin_PROGRAMS=mfsmount mfstools

AM_CPPFLAGS=-I$(top_srcdir)/mfscommon



mfsmount_LDADD=$(PTHREAD_LIBS) $(FUSE_LIBS)
mfsmount_CFLAGS=$(PTHREAD_CFLAGS) -D_USE_PTHREADS $(FUSE_CFLAGS) -DFUSE_USE_VERSION=26 -D__DARWIN_64_BIT_INO_T=0 -DAPPNAME=mfsmount
mfsmount_CPPFLAGS=$(PTHREAD_CPPFLAGS) -I$(top_srcdir)/mfscommon

mfsmount_SOURCES=\
	dirattrcache.c dirattrcache.h \
	mfs_fuse.c mfs_fuse.h \
	mfs_meta_fuse.c mfs_meta_fuse.h \
	mastercomm.c mastercomm.h \
	cscomm.c cscomm.h \
	readdata.c readdata.h \
	writedata.c writedata.h \
	csdb.c csdb.h \
	stats.c stats.h \
	main.c \
	../mfscommon/th_sem.c ../mfscommon/th_sem.h \
	../mfscommon/th_queue.c ../mfscommon/th_queue.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/md5.c ../mfscommon/md5.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/datapack.h \
	../mfscommon/MFSCommunication.h

mfstools_LDADD=-lm

mfstools_SOURCES=\
	mfstools.c \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/datapack.h \
	../mfscommon/MFSCommunication.h

EXTRA_DIST = $(bin_SCRIPTS)

bin_SCRIPTS=\
	mfssnapshot

mfstools_links=\
	mfscheckfile mfsdirinfo mfsfileinfo mfsfilerepair \
	mfsgetgoal mfsgettrashtime mfsrgetgoal mfsrgettrashtime \
	mfsrsetgoal mfsrsettrashtime mfssetgoal mfssettrashtime \
	mfsgeteattr mfsseteattr mfsdeleattr \
	mfsmakesnapshot mfsappendchunks

install-exec-hook:
	for l in $(mfstools_links) ; do \
		$(LN_S) -f mfstools $(DESTDIR)$(bindir)/$$l ; \
	done
