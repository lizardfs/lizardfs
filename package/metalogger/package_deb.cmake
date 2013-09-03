set(COMPONENT_NAME "metalogger")

configure_file(daemon.prerm.in deb.${COMPONENT_NAME}.prerm)
configure_file(daemon.postinst.in deb.${COMPONENT_NAME}.postinst)
set_deb_component_control_extra("metalogger" "postinst" "${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.postinst")
set_deb_component_control_extra("metalogger" "prerm" "${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.prerm")

