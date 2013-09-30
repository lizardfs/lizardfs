set(COMPONENT_NAME "client")

set(CPACK_DEB_${COMPONENT_NAME}_PACKAGE_DEPENDS "fuse")
set(CPACK_DEB_${COMPONENT_NAME}_PACKAGE_DESCRIPTION "LizardFS fuse client")

set_deb_component_control_extra(${COMPONENT_NAME} "postinst" "empty")
set_deb_component_control_extra(${COMPONENT_NAME} "prerm" "empty")
