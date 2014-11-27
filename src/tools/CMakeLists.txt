include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../common)
set(LFSTOOL_LINKS
  lfsgetgoal
  lfssetgoal
  lfsgettrashtime
  lfssettrashtime
  lfscheckfile
  lfsfileinfo
  lfsappendchunks
  lfsdirinfo
  lfsfilerepair
  lfsmakesnapshot
  lfsgeteattr
  lfsseteattr
  lfsdeleattr
  lfsrgetgoal
  lfsrsetgoal
  lfsrgettrashtime
  lfsrsettrashtime
  lfssetquota
  lfsrepquota)

collect_sources(TOOLS)

add_executable(lfstools ${TOOLS_MAIN})
target_link_libraries(lfstools lfscommon)
install(TARGETS lfstools DESTINATION ${BIN_SUBDIR})

foreach(LINK ${LFSTOOL_LINKS})
  add_custom_target(${LINK} ALL
      COMMAND ln -sf lfstools ${LINK}
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      DEPENDS lfstools)
  install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${LINK} DESTINATION ${BIN_SUBDIR})
endforeach()
