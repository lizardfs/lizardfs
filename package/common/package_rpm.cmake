set(CPACK_common_PACKAGE_DESCRIPTION_SUMMARY "LizardFS common files")

configure_file(common/postinst.in rpm.common.postinst)
configure_file(common/prerm.in rpm.common.prerm)

set(CPACK_RPM_common_PACKAGE_REQUIRES "coreutils gawk grep redhat-lsb shadow-utils" PARENT_SCOPE)
set(CPACK_RPM_common_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpm.common.postinst PARENT_SCOPE)
set(CPACK_RPM_common_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpm.common.prerm PARENT_SCOPE)
