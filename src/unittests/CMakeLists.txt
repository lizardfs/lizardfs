include_directories(${GTEST_INCLUDE_DIRS})
include_directories(../common)

add_executable(unittests main.cc)
message(STATUS "system: ${Boost_SYSTEM_LIBRARY}, filesystem: ${Boost_FILESYSTEM_LIBRARY}")
target_link_libraries(unittests gtest mfscommon
    "-Wl,--whole-archive" ${TEST_LIBRARIES} "-Wl,--no-whole-archive"
    ${Boost_SYSTEM_LIBRARY} ${Boost_FILESYSTEM_LIBRARY})
