include_directories(${GTEST_INCLUDE_DIRS})

collect_sources(UNITTESTS)
add_library(unittests-helpers ${UNITTESTS_SOURCES})

add_executable(unittests ${UNITTESTS_MAIN})
target_link_libraries(unittests gtest mfscommon
    "-Wl,--whole-archive" ${TEST_LIBRARIES} "-Wl,--no-whole-archive"
    unittests-helpers ${Boost_SYSTEM_LIBRARY} ${Boost_FILESYSTEM_LIBRARY})
