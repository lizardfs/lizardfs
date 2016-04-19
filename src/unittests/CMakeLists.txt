include_directories(${GTEST_INCLUDE_DIRS})

add_subdirectory(mocks)

collect_sources(UNITTESTS)
add_library(unittests-helpers ${UNITTESTS_SOURCES})
add_tests(unittests-helpers ${UNITTESTS_TESTS})

add_executable(unittests ${UNITTESTS_MAIN})
if(APPLE)
    set(OSX_TEST_LIBRARIES "")
    foreach(f ${TEST_LIBRARIES})
        list(APPEND OSX_TEST_LIBRARIES "-Wl,-force_load" "${f}")
    endforeach(f)
    target_link_libraries(unittests gtest mfscommon master ${OSX_TEST_LIBRARIES}
        unittests-helpers unittest-mocks ${Boost_SYSTEM_LIBRARY} ${Boost_FILESYSTEM_LIBRARY})
else()
    target_link_libraries(unittests gtest mfscommon master
        "-Wl,--whole-archive" ${TEST_LIBRARIES} "-Wl,--no-whole-archive"
        unittests-helpers unittest-mocks ${Boost_SYSTEM_LIBRARY} ${Boost_FILESYSTEM_LIBRARY})
endif()
