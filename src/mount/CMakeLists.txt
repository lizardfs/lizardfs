include_directories(${CMAKE_CURRENT_SOURCE_DIR})

collect_sources(MOUNT)

add_library(mount ${MOUNT_SOURCES})
target_link_libraries(mount mfscommon)
if(MINGW)
  target_link_libraries(mount ${MINGW_MSVCRT})
endif()

add_tests(mount ${MOUNT_TESTS})

install(PROGRAMS mfssnapshot DESTINATION ${BIN_SUBDIR})

