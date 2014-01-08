include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DFUSE_USE_VERSION=26)
add_definitions(-DAPPNAME=mfsmount)

collect_sources(MOUNT)

add_library(mount ${MOUNT_SOURCES})
target_link_libraries(mount mfscommon ${ZLIB_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT} ${ADDITIONAL_LIBS})
add_tests(mount ${MOUNT_TESTS})

add_executable(mfsmount ${MOUNT_MAIN})
target_link_libraries(mfsmount mount ${FUSE_LIBRARY})
install(TARGETS mfsmount RUNTIME DESTINATION ${BIN_SUBDIR})

install(PROGRAMS mfssnapshot DESTINATION ${BIN_SUBDIR})
