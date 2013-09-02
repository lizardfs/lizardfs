set(DAEMON_NAME "chunkserver")

configure_file(mfschunkserver.cfg.in mfschunkserver.cfg)
configure_file(${CMAKE_SOURCE_DIR}/mfsdata/default.daemon.in default @ONLY)
configure_file(${CMAKE_SOURCE_DIR}/mfsdata/init.daemon.in init @ONLY)

install(FILES mfshdd.cfg DESTINATION ${ETC_SUBDIR}/mfs RENAME mfshdd.cfg.dist COMPONENT chunkserver)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/mfschunkserver.cfg DESTINATION ${ETC_SUBDIR}/mfs RENAME mfschunkserver.cfg.dist COMPONENT chunkserver)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/init
        DESTINATION ${ETC_PATH}/init.d
        RENAME lizardfs-chunkserver
        COMPONENT chunkserver)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/default
        DESTINATION ${ETC_PATH}/default
        RENAME lizardfs-chunkserver 
        COMPONENT chunkserver)
