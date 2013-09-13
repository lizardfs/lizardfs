find_package(Boost REQUIRED)
find_package(GTest REQUIRED)

include_directories(../)
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${GTEST_INCLUDE_DIRS})

add_subdirectory(common)

