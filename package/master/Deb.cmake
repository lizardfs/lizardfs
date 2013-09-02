set(CPACK_master_PACKAGE_DESCRIPTION_SUMMARY "LizardFS master server")

# Set generator-dependent variables
if(BUILD_DEB)
  
endif()
if(BUILD_RPM)
  set(CPACK_RPM_master_PACKAGE_REQUIRES "filesystem lizardfs-common zlib" PARENT_SCOPE)
  set(CPACK_RPM_master_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/postinst PARENT_SCOPE)
  set(CPACK_RPM_master_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/prerm PARENT_SCOPE)

  set(CPACK_RPM_master_USER_FILELIST
      "%ignore /usr"        #Provided by filesystem
      "%ignore /usr/sbin"
      "%ignore ${DATA_PATH}" #Provided by lizardfs-common
      PARENT_SCOPE)
endif()

# Install
set(DAEMON_NAME "master")

install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/init 
        DESTINATION ${ETC_PATH}/init.d
        RENAME lizardfs-master
        COMPONENT master)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/default 
        DESTINATION ${ETC_PATH}/default
        RENAME lizardfs-master 
        COMPONENT master)
