include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../mfscommon)
add_definitions(-DFUSE_USE_VERSION=26)
add_definitions(-DAPPNAME=mfsmount)

aux_source_directory(. MOUNT_SOURCES)
add_executable(mfsmount ${MOUNT_SOURCES})
target_link_libraries(mfsmount mfscommon ${ZLIB_LIBRARIES} ${FUSE_LIBRARY} ${CMAKE_THREAD_LIBS_INIT})
install(TARGETS mfsmount DESTINATION bin)
