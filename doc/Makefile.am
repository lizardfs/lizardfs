general_mans=moosefs.7 mfs.7
chunkserver_mans=mfschunkserver.8
master_mans=mfsmaster.8 mfsmetarestore.8
mount_mans=\
	mfsmount.8 mfstools.1 \
	mfscheckfile.1 mfsdirinfo.1 mfsfileinfo.1 \
	mfsgetgoal.1 mfsgettrashtime.1 mfsrgetgoal.1 mfsrgettrashtime.1 \
	mfsrsetgoal.1 mfsrsettrashtime.1 mfssetgoal.1 mfssettrashtime.1 \
	mfssnapshot.1
all_mans=$(general_mans) $(chunkserver_mans) $(master_mans) $(mount_mans)

man_MANS=$(general_mans)
EXTRA_DIST=$(all_mans)

if BUILD_MASTER
man_MANS+=$(master_mans)
endif
if BUILD_CHUNKSERVER
man_MANS+=$(chunkserver_mans)
endif
if BUILD_MOUNT
man_MANS+=$(mount_mans)
endif

man-html: $(all_mans)
	$(MKDIR_P) html
	for f in $(all_mans) ; do \
		man2html -r $$f | tail -n +3 > html/$${f}.html ; \
	done
