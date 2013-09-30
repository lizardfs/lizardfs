set(COMPONENT_NAME "chunkserver")
set(DAEMON_NAME ${COMPONENT_NAME})

configure_file(mfschunkserver.cfg.in mfschunkserver.cfg)

append_file_to_var(INIT_DAEMON_INITIALIZE_EXTRA ../daemon.init.initializeDataDirectory.part)
configure_file(../daemon.init.in init @ONLY)
configure_file(../daemon.default.in default)

install(FILES mfshdd.cfg DESTINATION ${ETC_SUBDIR}/mfs RENAME mfshdd.cfg.dist COMPONENT ${COMPONENT_NAME})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/mfschunkserver.cfg
        DESTINATION ${ETC_SUBDIR}/mfs
        RENAME mfschunkserver.cfg.dist
        COMPONENT ${COMPONENT_NAME})
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/init
        DESTINATION ${ETC_SUBDIR}/init.d
        RENAME lizardfs-${DAEMON_NAME}
        COMPONENT ${COMPONENT_NAME})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/default
        DESTINATION ${ETC_SUBDIR}/default
        RENAME lizardfs-${DAEMON_NAME}
        COMPONENT ${COMPONENT_NAME})
