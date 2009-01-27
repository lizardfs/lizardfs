bin_PROGRAMS=mfsmount mfstools

AM_CPPFLAGS=-I$(top_srcdir) -D_REENTRANT -D_THREAD_SAFE -DFUSE_USE_VERSION=25

mfsmount_LDADD=$(FUSE_LIBS)
mfsmount_CFLAGS=$(FUSE_CFLAGS)

mfsmount_SOURCES=\
	mfs_fuse.c mfs_meta_fuse.c mastercomm.c cscomm.c crc.c sockets.c \
	readdata.c writedata.c csdb.c main.c \
	crc.h cscomm.h datapack.h mastercomm.h mfs_fuse.h mfs_meta_fuse.h \
	readdata.h sockets.h writedata.h csdb.h \
	../MFSCommunication.h

mfstools_SOURCES=\
	mfstools.c \
	datapack.h \
	../MFSCommunication.h

mfstools_links=\
	mfscheckfile mfsdirinfo mfsfileinfo \
	mfsgetgoal mfsgettrashtime mfsrgetgoal mfsrgettrashtime \
	mfsrsetgoal mfsrsettrashtime mfssetgoal mfssettrashtime \
	mfssnapshot

install-exec-hook:
	for l in $(mfstools_links) ; do \
		$(LN_S) -f mfstools $(DESTDIR)$(bindir)/$$l ; \
	done
