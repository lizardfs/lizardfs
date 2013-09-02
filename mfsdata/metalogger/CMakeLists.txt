set(DAEMON_NAME "metalogger")

configure_file(mfsmetalogger.cfg.in mfsmetalogger.cfg)
configure_file(${CMAKE_SOURCE_DIR}/mfsdata/default.daemon.in default @ONLY)
configure_file(${CMAKE_SOURCE_DIR}/mfsdata/init.daemon.in init @ONLY)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/mfsmetalogger.cfg DESTINATION ${ETC_SUBDIR}/mfs RENAME mfsmetalogger.cfg.dist COMPONENT metalogger)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/init
        DESTINATION ${ETC_PATH}/init.d
        RENAME lizardfs-metalogger
        COMPONENT metalogger)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/default
        DESTINATION ${ETC_PATH}/default
        RENAME lizardfs-metalogger 
        COMPONENT metalogger)
