set(COMPONENT_NAME "cgiserv")

set(CPACK_DEB_${COMPONENT_NAME}_PACKAGE_DEPENDS "lizardfs-common, lizardfs-cgi")
set(CPACK_DEB_${COMPONENT_NAME}_PACKAGE_DESCRIPTION "Simple CGI-capable HTTP server to run LizardFS CGI Monitor")
set(CPACK_DEB_${COMPONENT_NAME}_PACKAGE_ARCHITECTURE "all")

configure_file(daemon.deb.postinst.in deb.${COMPONENT_NAME}.postinst.in)
configure_file(daemon.deb.prerm.in deb.${COMPONENT_NAME}.prerm.in)
configure_file(${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.postinst.in deb.${COMPONENT_NAME}.postinst)
configure_file(${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.prerm.in deb.${COMPONENT_NAME}.prerm)
set_deb_component_control_extra(${COMPONENT_NAME} "postinst" "${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.postinst")
set_deb_component_control_extra(${COMPONENT_NAME} "prerm" "${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.prerm")
