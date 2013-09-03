set(DAEMON_NAME "master")

configure_file(daemon.prerm.in deb.master.prerm)
configure_file(daemon.postinst.in deb.master.postinst)
set_deb_component_control_extra("master" "postinst" "${CMAKE_CURRENT_BINARY_DIR}/deb.master.postinst")
set_deb_component_control_extra("master" "prerm" "${CMAKE_CURRENT_BINARY_DIR}/deb.master.prerm")

