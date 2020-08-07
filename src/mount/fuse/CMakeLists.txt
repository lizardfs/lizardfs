include_directories(${CMAKE_CURRENT_SOURCE_DIR})

collect_sources(MOUNT_FUSE)

if(FUSE_FOUND)
  add_executable(mfsmount ${MOUNT_FUSE_MAIN} ${MOUNT_FUSE_SOURCES})
  target_link_libraries(mfsmount mount mfscommon ${FUSE_LIBRARY})
  target_include_directories(mfsmount PRIVATE ${FUSE_INCLUDE_DIR})
  target_compile_definitions(mfsmount PRIVATE "-DAPPNAME=mfsmount" -DAPP_EXAMPLES_DIRECTORY="${CLIENT_EXAMPLES_SUBDIR}" ${FUSE_CFLAGS_OTHER} "-DFUSE_USE_VERSION=26")
  install(TARGETS mfsmount RUNTIME DESTINATION ${BIN_SUBDIR})
endif()

if(FUSE3_FOUND)
  add_executable(mfsmount3 ${MOUNT_FUSE_MAIN} ${MOUNT_FUSE_SOURCES})
  target_link_libraries(mfsmount3 mount mfscommon ${FUSE3_LIBRARY})
  target_include_directories(mfsmount3 PRIVATE ${FUSE3_INCLUDE_DIR})
  target_compile_definitions(mfsmount3 PRIVATE "-DAPPNAME=mfsmount3" "-DCFGNAME=mfsmount" -DAPP_EXAMPLES_DIRECTORY="${CLIENT_EXAMPLES_SUBDIR}" ${FUSE3_CFLAGS_OTHER} "-DFUSE_USE_VERSION=30")
  install(TARGETS mfsmount3 RUNTIME DESTINATION ${BIN_SUBDIR})
endif()
