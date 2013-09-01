set(CPACK_metalogger_PACKAGE_DESCRIPTION_SUMMARY "LizardFS metalogger")

set(CPACK_RPM_metalogger_PACKAGE_REQUIRES "filesystem lizardfs-common" PARENT_SCOPE)
set(CPACK_RPM_metalogger_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/postinst PARENT_SCOPE)
set(CPACK_RPM_metalogger_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/prerm PARENT_SCOPE)

set(CPACK_RPM_metalogger_USER_FILELIST
  "%ignore /usr"        #Provided by filesystem
  "%ignore /usr/sbin"
  "%ignore ${DATA_PATH}" #Provided by lizardfs-common
  PARENT_SCOPE)

set(DAEMON_NAME "metalogger")

configure_file(default.in default @ONLY)
configure_file(../init_daemon.in init @ONLY)
configure_file(../postinst_daemon.in postinst @ONLY)
configure_file(../prerm_daemon.in prerm @ONLY)

install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/init 
        DESTINATION ${ETC_PATH}/init.d
        RENAME lizardfs-metalogger
        COMPONENT metalogger)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/default 
        DESTINATION ${ETC_PATH}/default
        RENAME lizardfs-metalogger 
        COMPONENT metalogger)
