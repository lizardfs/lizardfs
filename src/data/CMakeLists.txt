configure_file(mfsmaster.cfg.in mfsmaster.cfg)
configure_file(mfschunkserver.cfg.in mfschunkserver.cfg)
configure_file(mfsmetalogger.cfg.in mfsmetalogger.cfg)
configure_file(postinst.in postinst)
set(POSTINST_SCRIPT ${CMAKE_CURRENT_BINARY_DIR}/postinst PARENT_SCOPE)

install(FILES metadata.mfs                                   DESTINATION ${DATA_SUBDIR} RENAME metadata.mfs.empty)
install(FILES mfsexports.cfg                                 DESTINATION ${MFSMASTER_EXAMPLES_SUBDIR})
install(FILES mfsgoals.cfg                                   DESTINATION ${MFSMASTER_EXAMPLES_SUBDIR})
install(FILES mfshdd.cfg                                     DESTINATION ${CHUNKSERVER_EXAMPLES_SUBDIR})
install(FILES mfsmount.cfg                                   DESTINATION ${CLIENT_EXAMPLES_SUBDIR})
install(FILES iolimits.cfg                                   DESTINATION ${CLIENT_EXAMPLES_SUBDIR})
install(FILES mfstopology.cfg                                DESTINATION ${MFSMASTER_EXAMPLES_SUBDIR})
install(FILES globaliolimits.cfg                             DESTINATION ${MFSMASTER_EXAMPLES_SUBDIR})
install(FILES lizardfs-uraft.cfg                             DESTINATION ${URAFT_EXAMPLES_SUBDIR})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/mfsmaster.cfg      DESTINATION ${MFSMASTER_EXAMPLES_SUBDIR})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/mfschunkserver.cfg DESTINATION ${CHUNKSERVER_EXAMPLES_SUBDIR})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/mfsmetalogger.cfg  DESTINATION ${METALOGGER_EXAMPLES_SUBDIR})

install(FILES lizardfs.completion DESTINATION etc/bash_completion.d RENAME lizardfs)

if(BUILD_TESTS)
  # Create a mock include dir.
  # We do this because we want to preserve the proper include directives in the example code
  # and still build this example program in the build step before the needed headers are installed.
  file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/include/lizardfs)
  add_custom_command(
    OUTPUT
      ${CMAKE_CURRENT_BINARY_DIR}/include/lizardfs/lizardfs_c_api.h
      ${CMAKE_CURRENT_BINARY_DIR}/include/lizardfs/lizardfs_error_codes.h
    COMMAND ${CMAKE_COMMAND} -E create_symlink
      ${CMAKE_SOURCE_DIR}/src/mount/client/lizardfs_c_api.h
      ${CMAKE_CURRENT_BINARY_DIR}/include/lizardfs/lizardfs_c_api.h
    COMMAND ${CMAKE_COMMAND} -E create_symlink
      ${CMAKE_SOURCE_DIR}/src/common/lizardfs_error_codes.h
      ${CMAKE_CURRENT_BINARY_DIR}/include/lizardfs/lizardfs_error_codes.h
  )
  add_executable(c-client-example
    liblizardfs-client-example.c
    ${CMAKE_CURRENT_BINARY_DIR}/include/lizardfs/lizardfs_c_api.h
    ${CMAKE_CURRENT_BINARY_DIR}/include/lizardfs/lizardfs_error_codes.h)
  target_include_directories(c-client-example PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/include)
  target_link_libraries(c-client-example lizardfs-client)
  install(TARGETS c-client-example RUNTIME DESTINATION ${BIN_SUBDIR})
endif()
