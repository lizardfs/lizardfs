include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DFUSE_USE_VERSION=26)
add_definitions(-DAPPNAME=mfsmount)

file(GLOB MOUNT_SOURCES *.h *.cc)
file(GLOB MOUNT_MAIN    main.cc)
list(REMOVE_ITEM MOUNT_SOURCES ${MOUNT_MAIN})

add_library(mount STATIC ${MOUNT_SOURCES})
target_link_libraries(mount mfscommon)

add_executable(mfsmount ${MOUNT_MAIN})
target_link_libraries(mfsmount mfscommon mount ${ZLIB_LIBRARIES} ${FUSE_LIBRARY} ${CMAKE_THREAD_LIBS_INIT})
install(TARGETS mfsmount DESTINATION ${BIN_SUBDIR})
