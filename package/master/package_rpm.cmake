set(COMPONENT_NAME "master")

set(CPACK_RPM_${COMPONENT_NAME}_PACKAGE_REQUIRES "filesystem lizardfs-common zlib")

configure_file(daemon.prerm.in rpm.${COMPONENT_NAME}.prerm)
configure_file(daemon.postinst.in rpm.${COMPONENT_NAME}.postinst)
set(CPACK_RPM_${COMPONENT_NAME}_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpm.${COMPONENT_NAME}.postinst)
set(CPACK_RPM_${COMPONENT_NAME}_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpm.${COMPONENT_NAME}.prerm)

set(CPACK_RPM_${COMPONENT_NAME}_USER_FILELIST
    "%ignore /usr"        #Provided by filesystem
    "%ignore /usr/sbin"
    "%ignore ${DATA_PATH}" #Provided by lizardfs-common
)

