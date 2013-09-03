set(COMPONENT_NAME "cgiserv")

set(CPACK_${COMPONENT_NAME}_PACKAGE_DESCRIPTION_SUMMARY "LizardFS web client server (cgiserv)")

set(CPACK_RPM_${COMPONENT_NAME}_PACKAGE_REQUIRES "filesystem lizardfs-common" PARENT_SCOPE)
set(CPACK_RPM_${COMPONENT_NAME}_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/postinst PARENT_SCOPE)
set(CPACK_RPM_${COMPONENT_NAME}_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/prerm PARENT_SCOPE)

set(CPACK_RPM_${COMPONENT_NAME}_USER_FILELIST
  "%ignore /usr"        #Provided by filesystem
  "%ignore /usr/sbin"
  "%ignore ${DATA_PATH}" #Provided by lizardfs-common
  PARENT_SCOPE)

configure_file(postinst.daemon.in postinst.${COMPONENT_NAME} @ONLY)
configure_file(prerm.daemon.in prerm.${COMPONENT_NAME} @ONLY)
