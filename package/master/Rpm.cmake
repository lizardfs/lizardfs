set(DAEMON_NAME "master")

set(CPACK_master_PACKAGE_DESCRIPTION_SUMMARY "LizardFS master server")

set(CPACK_RPM_master_PACKAGE_REQUIRES "filesystem lizardfs-common zlib" PARENT_SCOPE)
set(CPACK_RPM_master_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/postinst.master PARENT_SCOPE)
set(CPACK_RPM_master_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/prerm.master PARENT_SCOPE)

set(CPACK_RPM_master_USER_FILELIST
    "%ignore /usr"        #Provided by filesystem
    "%ignore /usr/sbin"
    "%ignore ${DATA_PATH}" #Provided by lizardfs-common
    PARENT_SCOPE)

configure_file(postinst.daemon.in postinst.master @ONLY)
configure_file(prerm.daemon.in prerm.master @ONLY)