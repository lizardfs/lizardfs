set(COMPONENT_NAME "master")

set(CPACK_DEB_${COMPONENT_NAME}_PACKAGE_DEPENDS "lizardfs-common, zlib1g")
set(CPACK_DEB_${COMPONENT_NAME}_PACKAGE_DESCRIPTION "LizardFS metadata server")

configure_file(daemon.deb.postinst.in deb.${COMPONENT_NAME}.postinst.in)
configure_file(daemon.deb.prerm.in deb.${COMPONENT_NAME}.prerm.in)
configure_file(${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.postinst.in deb.${COMPONENT_NAME}.postinst)
configure_file(${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.prerm.in deb.${COMPONENT_NAME}.prerm)
set_deb_component_control_extra(${COMPONENT_NAME} "postinst" "${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.postinst")
set_deb_component_control_extra(${COMPONENT_NAME} "prerm" "${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.prerm")
