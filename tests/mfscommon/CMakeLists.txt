include_directories(${Boost_INCLUDE_DIRS})

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} COMMONTESTS_SOURCES)
add_executable(commontests ${COMMONTESTS_SOURCES})
target_link_libraries(commontests gtest gtest_main mfscommon testcommon)
