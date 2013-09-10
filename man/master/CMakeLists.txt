install(FILES
        mfsmaster.8
        mfsmetarestore.8
        DESTINATION ${MAN_SUBDIR}/man8 COMPONENT master)
install(FILES
        mfsmaster.cfg.5
        mfsexports.cfg.5
        mfstopology.cfg.5
        DESTINATION ${MAN_SUBDIR}/man5 COMPONENT master)

