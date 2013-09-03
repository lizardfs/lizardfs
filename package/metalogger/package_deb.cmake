set(DAEMON_NAME "metalogger")

configure_file(daemon.prerm.in deb.metalogger.prerm)
configure_file(daemon.postinst.in deb.metalogger.postinst)
set_deb_component_control_extra("metalogger" "postinst" "${CMAKE_CURRENT_BINARY_DIR}/deb.metalogger.postinst")
set_deb_component_control_extra("metalogger" "prerm" "${CMAKE_CURRENT_BINARY_DIR}/deb.metalogger.prerm")

