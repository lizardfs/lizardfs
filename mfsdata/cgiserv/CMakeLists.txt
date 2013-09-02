set(DAEMON_NAME "cgiserv")

configure_file(${CMAKE_SOURCE_DIR}/mfsdata/default.daemon.in default @ONLY)
configure_file(${CMAKE_SOURCE_DIR}/mfsdata/init.daemon.in init @ONLY)

install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/init
        DESTINATION ${ETC_PATH}/init.d
        RENAME lizardfs-cgiserv
        COMPONENT cgiserv)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/default
        DESTINATION ${ETC_PATH}/default
        RENAME lizardfs-cgiserv
        COMPONENT cgiserv)
