set(GTEST_ROOT ${CMAKE_SOURCE_DIR}/tests/googletest CACHE PATH "Path to googletest sources")
add_subdirectory(${GTEST_ROOT})
include_directories(${GTEST_ROOT}/include)

add_subdirectory(common)
add_subdirectory(chunkserver_tests)