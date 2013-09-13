aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} SOURCES)
add_executable(test_common_charts ${SOURCES})
target_link_libraries(test_common_charts mfscommon ${GTEST_BOTH_LIBRARIES} pthread)
