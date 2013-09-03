set(COMPONENT_NAME "common")
configure_file(${COMPONENT_NAME}/prerm.in deb.${COMPONENT_NAME}.prerm)
configure_file(${COMPONENT_NAME}/postinst.in deb.${COMPONENT_NAME}.postinst)

set_deb_component_control_extra(${COMPONENT_NAME} "postinst" "${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.postinst")
set_deb_component_control_extra(${COMPONENT_NAME} "prerm" "${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.prerm")
