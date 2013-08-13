include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../mfscommon)
set(MFSTOOL_LINKS
  mfsgetgoal
  mfssetgoal
  mfsgettrashtime
  mfssettrashtime
  mfscheckfile
  mfsfileinfo
  mfsappendchunks
  mfsdirinfo
  mfsfilerepair
  mfsmakesnapshot
  mfsgeteattr
  mfsseteattr
  mfsdeleattr)

aux_source_directory(. TOOLS_SOURCES)
add_executable(mfstools ${TOOLS_SOURCES})
target_link_libraries(mfstools mfscommon)
install(TARGETS mfstools DESTINATION ${BIN_SUBDIR})

foreach(LINK ${MFSTOOL_LINKS})
  add_custom_target(${LINK} ALL
      COMMAND ln -sf mfstools ${LINK}
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      DEPENDS mfstools)
  install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${LINK} DESTINATION ${BIN_SUBDIR})
endforeach()
