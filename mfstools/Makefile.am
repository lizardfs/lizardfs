bin_PROGRAMS=mfstools

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
