include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_definitions(-DAPPNAME=lfsmount)

collect_sources(MOUNT_FUSE)

add_library(mount_fuse ${MOUNT_FUSE_SOURCES})
target_link_libraries(mount_fuse mount lfscommon)

add_definitions(-DFUSE_USE_VERSION=26)
add_executable(lfsmount ${MOUNT_FUSE_MAIN})
target_link_libraries(lfsmount mount_fuse ${FUSE_LIBRARY})
install(TARGETS lfsmount RUNTIME DESTINATION ${BIN_SUBDIR})

