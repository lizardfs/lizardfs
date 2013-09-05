set(COMPONENT_NAME "metalogger")

set(CPACK_DEB_${COMPONENT_NAME}_PACKAGE_DEPENDS "lizardfs-common")
set(CPACK_DEB_${COMPONENT_NAME}_PACKAGE_DESCRIPTION "LizardFS metalogger server")

configure_file(daemon.prerm.in deb.${COMPONENT_NAME}.prerm)
configure_file(daemon.postinst.in deb.${COMPONENT_NAME}.postinst)
set_deb_component_control_extra(${COMPONENT_NAME} "postinst" "${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.postinst")
set_deb_component_control_extra(${COMPONENT_NAME} "prerm" "${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.prerm")
