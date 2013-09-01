configure_file(default.mfsmaster.in default.mfsmaster)
configure_file(mfsmaster.cfg.in mfsmaster.cfg)
configure_file(mfschunkserver.cfg.in mfschunkserver.cfg)
configure_file(mfsmetalogger.cfg.in mfsmetalogger.cfg)

install(FILES metadata.mfs DESTINATION ${DATA_PATH} RENAME metadata.mfs.empty COMPONENT master)
install(FILES mfsexports.cfg DESTINATION ${ETC_PATH}/mfs RENAME mfsexports.cfg.dist COMPONENT master)
install(FILES mfshdd.cfg DESTINATION ${ETC_SUBDIR}/mfs RENAME mfshdd.cfg.dist COMPONENT chunkserver)
install(FILES mfsmount.cfg DESTINATION ${ETC_SUBDIR}/mfs RENAME mfsmount.cfg.dist COMPONENT client)
install(FILES mfstopology.cfg DESTINATION ${ETC_PATH}/mfs RENAME mfstopology.cfg.dist COMPONENT master)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/default.mfsmaster
        DESTINATION ${ETC_PATH}/default
        RENAME lizardfs-master
        COMPONENT master)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/mfsmaster.cfg DESTINATION ${ETC_PATH}/mfs RENAME mfsmaster.cfg.dist COMPONENT master)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/mfschunkserver.cfg DESTINATION ${ETC_SUBDIR}/mfs RENAME mfschunkserver.cfg.dist COMPONENT chunkserver)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/mfsmetalogger.cfg DESTINATION ${ETC_SUBDIR}/mfs RENAME mfsmetalogger.cfg.dist COMPONENT metalogger)
