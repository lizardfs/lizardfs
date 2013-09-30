set(COMPONENT_NAME "cgi")

set(CPACK_DEB_${COMPONENT_NAME}_PACKAGE_DESCRIPTION "LizardFS CGI Monitor")
set(CPACK_DEB_${COMPONENT_NAME}_PACKAGE_ARCHITECTURE "all")

set_deb_component_control_extra(${COMPONENT_NAME} "postinst" "empty")
set_deb_component_control_extra(${COMPONENT_NAME} "prerm" "empty")
