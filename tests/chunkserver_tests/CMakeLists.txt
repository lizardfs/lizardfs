include_directories(${Boost_INCLUDE_DIRS})

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} CHUNKSERVERTESTS_SOURCES)
add_executable(chunkservertests ${CHUNKSERVERTESTS_SOURCES})
target_link_libraries(chunkservertests gtest gtest_main chunkserver testcommon)
