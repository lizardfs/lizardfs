set(DAEMON_NAME "master")

configure_file(mfsmaster.cfg.in mfsmaster.cfg)
configure_file(${CMAKE_SOURCE_DIR}/mfsdata/default.daemon.in default @ONLY)
configure_file(${CMAKE_SOURCE_DIR}/mfsdata/init.daemon.in init @ONLY)

install(FILES metadata.mfs DESTINATION ${DATA_PATH} RENAME metadata.mfs.empty COMPONENT master)
install(FILES mfsexports.cfg DESTINATION ${ETC_PATH}/mfs RENAME mfsexports.cfg.dist COMPONENT master)
install(FILES mfstopology.cfg DESTINATION ${ETC_PATH}/mfs RENAME mfstopology.cfg.dist COMPONENT master)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/mfsmaster.cfg DESTINATION ${ETC_PATH}/mfs RENAME mfsmaster.cfg.dist COMPONENT master)

install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/init
        DESTINATION ${ETC_PATH}/init.d
        RENAME lizardfs-master
        COMPONENT master)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/default
        DESTINATION ${ETC_PATH}/default
        RENAME lizardfs-master 
        COMPONENT master)
