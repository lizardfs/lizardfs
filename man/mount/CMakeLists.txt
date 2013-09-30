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

