set(COMPONENT_NAME "cgiserv")

set(CPACK_${COMPONENT_NAME}_PACKAGE_DESCRIPTION_SUMMARY "LizardFS web client server (cgiserv)")
set(CPACK_RPM_${COMPONENT_NAME}_PACKAGE_REQUIRES "filesystem lizardfs-common" PARENT_SCOPE)

configure_file(daemon.postinst.in rpm.${COMPONENT_NAME}.postinst @ONLY)
configure_file(daemon.prerm.in rpm.${COMPONENT_NAME}.prerm @ONLY)
set(CPACK_RPM_${COMPONENT_NAME}_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpm.${COMPONENT_NAME}.postinst PARENT_SCOPE)
set(CPACK_RPM_${COMPONENT_NAME}_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpm.${COMPONENT_NAME}.prerm PARENT_SCOPE)
