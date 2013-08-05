add_definitions(-DAPPNAME=mfsmount -DFUSE_USE_VERSION=26)

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} MFSMOUNT_SOURCES)
add_executable(mfsmount ${MFSMOUNT_SOURCES})
target_link_libraries(mfsmount mfscommon ${M_LIB} ${Z_LIB} ${FUSE_LIB} ${DL_LIB})
install(TARGETS mfsmount RUNTIME DESTINATION bin)
