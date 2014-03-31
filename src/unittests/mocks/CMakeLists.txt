include_directories(${GTEST_INCLUDE_DIRS})

collect_sources(UNITTESTMOCKS)
add_library(unittest-mocks ${UNITTESTMOCKS_SOURCES})
add_tests(unittest-mocks ${UNITTESTMOCKS_TESTS})