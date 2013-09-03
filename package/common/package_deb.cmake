configure_file(common/prerm.in deb.common.prerm)
configure_file(common/postinst.in deb.common.postinst)

set_deb_component_control_extra("common" "postinst" "${CMAKE_CURRENT_BINARY_DIR}/deb.common.postinst")
set_deb_component_control_extra("common" "prerm" "${CMAKE_CURRENT_BINARY_DIR}/deb.common.prerm")
