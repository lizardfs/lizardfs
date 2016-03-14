include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_definitions(-DAPPNAME=mfsmount)

collect_sources(MOUNT_FUSE)

add_library(mount_fuse ${MOUNT_FUSE_SOURCES})
target_link_libraries(mount_fuse mount mfscommon)

add_definitions(-DFUSE_USE_VERSION=26)
add_definitions(${FUSE_CFLAGS_OTHER})
include_directories(${FUSE_INCLUDE_DIR})
add_executable(mfsmount ${MOUNT_FUSE_MAIN})
target_link_libraries(mfsmount mount_fuse ${FUSE_LIBRARY})
install(TARGETS mfsmount RUNTIME DESTINATION ${BIN_SUBDIR})

