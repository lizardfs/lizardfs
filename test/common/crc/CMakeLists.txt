aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} SOURCES)
add_executable(test_crc ${SOURCES})
target_link_libraries(test_crc mfscommon ${GTEST_BOTH_LIBRARIES} pthread)
