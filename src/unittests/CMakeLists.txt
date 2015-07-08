include_directories(${GTEST_INCLUDE_DIRS})

add_subdirectory(mocks)

collect_sources(UNITTESTS)
add_library(unittests-helpers ${UNITTESTS_SOURCES})
add_tests(unittests-helpers ${UNITTESTS_TESTS})

add_executable(unittests ${UNITTESTS_MAIN})
target_link_libraries(unittests gtest mfscommon
    "-Wl,--whole-archive" ${TEST_LIBRARIES} "-Wl,--no-whole-archive"
    unittests-helpers unittest-mocks ${Boost_SYSTEM_LIBRARY} ${Boost_FILESYSTEM_LIBRARY})
