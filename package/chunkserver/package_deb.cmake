set(COMPONENT_NAME "chunkserver")

set(CPACK_${COMPONENT_NAME}_PACKAGE_DESCRIPTION_SUMMARY "LizardFS chunk server")

set(CPACK_RPM_${COMPONENT_NAME}_PACKAGE_REQUIRES "filesystem lizardfs-common" PARENT_SCOPE)
set(CPACK_RPM_${COMPONENT_NAME}_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/postinst PARENT_SCOPE)
set(CPACK_RPM_${COMPONENT_NAME}_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/prerm PARENT_SCOPE)

set(CPACK_RPM_${COMPONENT_NAME}_USER_FILELIST
  "%ignore /usr"        #Provided by filesystem
  "%ignore /usr/sbin"
  "%ignore ${DATA_PATH}" #Provided by lizardfs-common
  PARENT_SCOPE)


configure_file(${CMAKE_SOURCE_DIR}/mfsdata/default.daemon.in default @ONLY)
configure_file(${CMAKE_SOURCE_DIR}/mfsdata/init.daemon.in init @ONLY)
configure_file(${CMAKE_SOURCE_DIR}/mfsdata/postinst.daemon.in postinst @ONLY)
configure_file(${CMAKE_SOURCE_DIR}/mfsdata/prerm.daemon.in prerm @ONLY)

install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/init 
        DESTINATION ${ETC_PATH}/init.d
        RENAME lizardfs-${COMPONENT_NAME}
        COMPONENT chunkserver)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/default 
        DESTINATION ${ETC_PATH}/default
        RENAME lizardfs-${COMPONENT_NAME}
        COMPONENT chunkserver)
