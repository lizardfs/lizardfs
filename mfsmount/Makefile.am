bin_PROGRAMS=mfsmount mfstools

AM_CPPFLAGS=-I$(top_srcdir)/mfscommon

mfsmount_LDADD=$(PTHREAD_LIBS) $(FUSE_LIBS)
mfsmount_CPPFLAGS=$(PTHREAD_CPPFLAGS) -D_USE_PTHREADS $(FUSE_CFLAGS) -DFUSE_USE_VERSION=26 -DAPPNAME=mfsmount -I$(top_srcdir)/mfscommon

mfsmount_SOURCES=\
	dirattrcache.cc dirattrcache.h \
	symlinkcache.cc symlinkcache.h \
	chunkloccache.cc chunkloccache.h \
	mfs_fuse.cc mfs_fuse.h \
	mfs_meta_fuse.cc mfs_meta_fuse.h \
	mastercomm.cc mastercomm.h \
	masterproxy.cc masterproxy.h \
	cscomm.cc cscomm.h \
	readdata.cc readdata.h \
	writedata.cc writedata.h \
	csdb.cc csdb.h \
	stats.cc stats.h \
	oplog.cc oplog.h \
	main.cc \
	../mfscommon/pcqueue.cc ../mfscommon/pcqueue.h \
	../mfscommon/crc.cc ../mfscommon/crc.h \
	../mfscommon/md5.cc ../mfscommon/md5.h \
	../mfscommon/sockets.cc ../mfscommon/sockets.h \
	../mfscommon/strerr.cc ../mfscommon/strerr.h \
	../mfscommon/datapack.h ../mfscommon/massert.h \
	../mfscommon/mfsstrerr.h \
	../mfscommon/MFSCommunication.h

mfstools_SOURCES=\
	mfstools.cc \
	../mfscommon/sockets.cc ../mfscommon/sockets.h \
	../mfscommon/strerr.cc ../mfscommon/strerr.h \
	../mfscommon/datapack.h ../mfscommon/mfsstrerr.h \
	../mfscommon/MFSCommunication.h

EXTRA_DIST = $(bin_SCRIPTS)

bin_SCRIPTS=\
	mfssnapshot

if INSTALL_QUOTA_TOOLS
mfstools_links=\
	mfscheckfile mfsdirinfo mfsfileinfo mfsfilerepair \
	mfsgetgoal mfsgettrashtime mfsrgetgoal mfsrgettrashtime \
	mfsrsetgoal mfsrsettrashtime mfssetgoal mfssettrashtime \
	mfsgeteattr mfsseteattr mfsdeleattr \
	mfsgetquota mfssetquota mfsdelquota \
	mfsmakesnapshot mfsappendchunks
else
mfstools_links=\
	mfscheckfile mfsdirinfo mfsfileinfo mfsfilerepair \
	mfsgetgoal mfsgettrashtime mfsrgetgoal mfsrgettrashtime \
	mfsrsetgoal mfsrsettrashtime mfssetgoal mfssettrashtime \
	mfsgeteattr mfsseteattr mfsdeleattr \
	mfsmakesnapshot mfsappendchunks
endif

install-exec-hook:
	for l in $(mfstools_links) ; do \
		$(LN_S) -f mfstools $(DESTDIR)$(bindir)/$$l ; \
	done
