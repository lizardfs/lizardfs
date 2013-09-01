set(CPACK_common_PACKAGE_DESCRIPTION_SUMMARY "LizardFS common files")

set(CPACK_RPM_common_PACKAGE_REQUIRES "coreutils gawk grep redhat-lsb shadow-utils" PARENT_SCOPE)
set(CPACK_RPM_common_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/postinst PARENT_SCOPE)
set(CPACK_RPM_common_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/prerm PARENT_SCOPE)

configure_file(prerm.in prerm)
configure_file(postinst.in postinst)

#Fix forcing generating package by putting dummy file
install(FILES daemon.touch DESTINATION ${ETC_PATH}/mfs COMPONENT common)
