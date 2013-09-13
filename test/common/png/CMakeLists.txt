aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} SOURCES)
add_executable(test_png ${SOURCES})
target_link_libraries(test_png ${ZLIB_LIBRARIES} ${GTEST_BOTH_LIBRARIES} pthread)
