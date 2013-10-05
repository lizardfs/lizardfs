include_directories(${Boost_INCLUDE_DIRS})

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} MOUNTTESTS_SOURCES)
add_executable(mounttests ${MOUNTTESTS_SOURCES})
target_link_libraries(mounttests gtest mfscommon testcommon mount)
