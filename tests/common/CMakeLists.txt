include_directories(${Boost_INCLUDE_DIRS})

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} TESTCOMMON_SOURCES)
add_library(testcommon ${TESTCOMMON_SOURCES})
target_link_libraries(testcommon ${Boost_FILESYSTEM_LIBRARY} ${Boost_SYSTEM_LIBRARY})
