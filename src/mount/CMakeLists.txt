include_directories(${CMAKE_CURRENT_SOURCE_DIR})

collect_sources(MOUNT)

add_library(mount ${MOUNT_SOURCES})
target_link_libraries(mount mfscommon ${CMAKE_THREAD_LIBS_INIT})
add_tests(mount ${MOUNT_TESTS})

install(PROGRAMS mfssnapshot DESTINATION ${BIN_SUBDIR})

