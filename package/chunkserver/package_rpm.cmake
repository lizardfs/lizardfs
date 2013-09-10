set(COMPONENT_NAME "chunkserver")

set(CPACK_RPM_${COMPONENT_NAME}_PACKAGE_REQUIRES "lizardfs-common")

configure_file(daemon.rpm.prerm.in rpm.${COMPONENT_NAME}.prerm.in)
configure_file(daemon.rpm.postinst.in rpm.${COMPONENT_NAME}.postinst.in)
configure_file(${CMAKE_CURRENT_BINARY_DIR}/rpm.${COMPONENT_NAME}.postinst.in rpm.${COMPONENT_NAME}.postinst)
configure_file(${CMAKE_CURRENT_BINARY_DIR}/rpm.${COMPONENT_NAME}.prerm.in rpm.${COMPONENT_NAME}.prerm)

set(CPACK_RPM_${COMPONENT_NAME}_PACKAGE_REQUIRES "filesystem lizardfs-common")
set(CPACK_RPM_${COMPONENT_NAME}_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpm.${COMPONENT_NAME}.postinst)
set(CPACK_RPM_${COMPONENT_NAME}_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpm.${COMPONENT_NAME}.prerm)

set(CPACK_RPM_${COMPONENT_NAME}_USER_FILELIST
    "%ignore /etc/init.d" #Provided by chkconfig
    "%ignore /usr"        #Provided by filesystem
    "%ignore /usr/sbin"
    "%ignore ${DATA_PATH}" #Provided by lizardfs-common
)

