include_directories(${CMAKE_CURRENT_SOURCE_DIR})
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
  mfsdeleattr
  mfsrgetgoal
  mfsrsetgoal
  mfsrgettrashtime
  mfsrsettrashtime)

collect_sources(TOOLS)

add_executable(mfstools ${TOOLS_MAIN})
target_link_libraries(mfstools mfscommon)
install(TARGETS mfstools DESTINATION ${BIN_SUBDIR})

foreach(LINK ${MFSTOOL_LINKS})
  add_custom_target(${LINK} ALL
      COMMAND ln -sf mfstools ${LINK}
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      DEPENDS mfstools)
  install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${LINK} DESTINATION ${BIN_SUBDIR})
endforeach()
