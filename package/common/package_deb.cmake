set(COMPONENT_NAME "common")

set(CPACK_DEB_${COMPONENT_NAME}_PACKAGE_DEPENDS "passwd, coreutils, grep")
set(CPACK_DEB_${COMPONENT_NAME}_PACKAGE_DESCRIPTION "Files and services common for all LizardFS daemons")
set(CPACK_DEB_${COMPONENT_NAME}_PACKAGE_ARCHITECTURE "all")

configure_file(${COMPONENT_NAME}/prerm.in deb.${COMPONENT_NAME}.prerm)
configure_file(${COMPONENT_NAME}/postinst.in deb.${COMPONENT_NAME}.postinst)

# Read basic config file
file(READ "${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.postinst" POSTINST_FILE)

# Prefix and suffix for install option
set(POSTINST_FILE "case \"\${1}\" in\nconfigure)\n${POSTINST_FILE}")
set(POSTINST_FILE "${POSTINST_FILE}\;\;\n *)\n\;\;\n esac\n")

# Save customized file
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.postinst" ${POSTINST_FILE})

set_deb_component_control_extra(${COMPONENT_NAME} "postinst" "${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.postinst")
set_deb_component_control_extra(${COMPONENT_NAME} "prerm" "${CMAKE_CURRENT_BINARY_DIR}/deb.${COMPONENT_NAME}.prerm")
