include_directories(${CMAKE_CURRENT_SOURCE_DIR})

collect_sources(MOUNT)

add_library(mount ${MOUNT_SOURCES})
target_link_libraries(mount mfscommon ${ADDITIONAL_LIBS} ${Boost_THREAD_LIBRARY})

create_unittest(mount ${MOUNT_TESTS})
link_unittest(mount mount mfscommon)

install(PROGRAMS mfssnapshot DESTINATION ${BIN_SUBDIR})

