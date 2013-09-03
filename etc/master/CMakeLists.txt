set(COMPONENT_NAME "master")
set(DAEMON_NAME ${COMPONENT_NAME})

configure_file(mfsmaster.cfg.in mfsmaster.cfg)

append_file_to_var(INIT_DAEMON_INITIALIZE_EXTRA ../daemon.init.initializeDataDirectory.part)
append_file_to_var(INIT_DAEMON_INITIALIZE_EXTRA daemon.init.initializeMetadataFile.part)
configure_file(../daemon.init.in init @ONLY)
configure_file(../daemon.default.in default)

install(FILES metadata.mfs DESTINATION ${DATA_PATH} RENAME metadata.mfs.empty COMPONENT ${COMPONENT_NAME})
install(FILES mfsexports.cfg DESTINATION ${ETC_PATH}/mfs RENAME mfsexports.cfg.dist COMPONENT ${COMPONENT_NAME})
install(FILES mfstopology.cfg DESTINATION ${ETC_PATH}/mfs RENAME mfstopology.cfg.dist COMPONENT ${COMPONENT_NAME})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/mfsmaster.cfg DESTINATION ${ETC_PATH}/mfs RENAME mfsmaster.cfg.dist COMPONENT ${COMPONENT_NAME})

install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/init
        DESTINATION ${ETC_PATH}/init.d
        RENAME lizardfs-${DAEMON_NAME}
        COMPONENT ${COMPONENT_NAME})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/default
        DESTINATION ${ETC_PATH}/default
        RENAME lizardfs-${DAEMON_NAME}
        COMPONENT ${COMPONENT_NAME})
