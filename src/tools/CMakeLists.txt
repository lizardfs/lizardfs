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
  mfsrsettrashtime
  mfssetquota
  mfsrepquota)

collect_sources(TOOLS)

add_executable(lizardfs ${TOOLS_MAIN} ${TOOLS_SOURCES})
target_link_libraries(lizardfs mfscommon)
install(TARGETS lizardfs DESTINATION ${BIN_SUBDIR})

install(FILES mfstools.sh DESTINATION ${BIN_SUBDIR} PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
                    GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ)

foreach(LINK ${MFSTOOL_LINKS})
  add_custom_target(${LINK} ALL
      COMMAND ln -sf mfstools.sh ${LINK}
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
  install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${LINK} DESTINATION ${BIN_SUBDIR})
endforeach()
