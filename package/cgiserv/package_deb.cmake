set(COMPONENT_NAME "cgiserv")
set(CPACK_${COMPONENT_NAME}_PACKAGE_DESCRIPTION_SUMMARY "LizardFS web client server (cgiserv)")

configure_file(${CMAKE_SOURCE_DIR}/mfsdata/default.daemon.in default @ONLY)
configure_file(${CMAKE_SOURCE_DIR}/mfsdata/init.daemon.in init @ONLY)
configure_file(${CMAKE_SOURCE_DIR}/mfsdata/postinst.daemon.in postinst @ONLY)
configure_file(${CMAKE_SOURCE_DIR}/mfsdata/prerm.daemon.in prerm @ONLY)

install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/init 
        DESTINATION ${ETC_PATH}/init.d
        RENAME lizardfs-cgiserv
        COMPONENT cgiserv)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/default 
        DESTINATION ${ETC_PATH}/default
        RENAME lizardfs-cgiserv 
        COMPONENT cgiserv)
