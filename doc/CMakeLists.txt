# general #
###########
install(FILES
        mfs.7
        moosefs.7
        DESTINATION ${MAN_SUBDIR}/man7 COMPONENT common)

# chunkserver #
###############
install(FILES
        mfschunkserver.8
        DESTINATION ${MAN_SUBDIR}/man8 COMPONENT chunkserver)
install(FILES
        mfschunkserver.cfg.5
        mfshdd.cfg.5
        DESTINATION ${MAN_SUBDIR}/man5 COMPONENT chunkserver)

# master #
##########
install(FILES
        mfsmaster.8
        mfsmetarestore.8
        mfsmetalogger.8
        mfscgiserv.8
        DESTINATION ${MAN_SUBDIR}/man8 COMPONENT master)
install(FILES
        mfsmaster.cfg.5
        mfsexports.cfg.5
        mfstopology.cfg.5
        mfsmetalogger.cfg.5
        DESTINATION ${MAN_SUBDIR}/man5 COMPONENT master)

# mount #
#########
install(FILES
        mfsmount.8
        DESTINATION ${MAN_SUBDIR}/man8 COMPONENT client)
install(FILES
        mfstools.1
        mfscheckfile.1
        mfsdirinfo.1
        mfsfileinfo.1
        mfsfilerepair.1
        mfsgetgoal.1
        mfsgettrashtime.1
        mfsrgetgoal.1
        mfsrgettrashtime.1
        mfsrsetgoal.1
        mfsrsettrashtime.1
        mfssetgoal.1
        mfssettrashtime.1
        mfsgeteattr.1
        mfsseteattr.1
        mfsdeleattr.1
        mfsappendchunks.1
        mfsmakesnapshot.1
        DESTINATION ${MAN_SUBDIR}/man1 COMPONENT client)
