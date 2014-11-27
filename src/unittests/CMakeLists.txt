include_directories(${GTEST_INCLUDE_DIRS})
include_directories(../common)

add_executable(unittests main.cc)
target_link_libraries(unittests gtest lfscommon
    "-Wl,--whole-archive" ${TEST_LIBRARIES} "-Wl,--no-whole-archive")
