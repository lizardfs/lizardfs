set(COMPONENT_NAME "chunkserver")

set(CPACK_RPM_${COMPONENT_NAME}_PACKAGE_REQUIRES "lizardfs-common")

configure_file(daemon.postinst.in rpm.${COMPONENT_NAME}.postinst @ONLY)
configure_file(daemon.prerm.in rpm.${COMPONENT_NAME}.prerm @ONLY)

set(CPACK_RPM_${COMPONENT_NAME}_PACKAGE_REQUIRES "filesystem lizardfs-common")
set(CPACK_RPM_${COMPONENT_NAME}_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpm.${COMPONENT_NAME}.postinst)
set(CPACK_RPM_${COMPONENT_NAME}_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpm.${COMPONENT_NAME}.prerm)
