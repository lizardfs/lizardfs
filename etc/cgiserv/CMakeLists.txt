set(COMPONENT_NAME "cgiserv")
set(DAEMON_NAME ${COMPONENT_NAME})

set(INIT_DAEMON_INITIALIZE_EXTRA "")
configure_file(../daemon.init.in init @ONLY)
configure_file(../daemon.default.in default)

install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/init
        DESTINATION ${ETC_PATH}/init.d
        RENAME lizardfs-${DAEMON_NAME}
        COMPONENT ${COMPONENT_NAME})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/default
        DESTINATION ${ETC_PATH}/default
        RENAME lizardfs-${DAEMON_NAME}
        COMPONENT ${COMPONENT_NAME})
