aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} COMMONTESTS_SOURCES)
add_executable(commontests ${COMMONTESTS_SOURCES})
target_link_libraries(commontests gtest mfscommon testcommon)
