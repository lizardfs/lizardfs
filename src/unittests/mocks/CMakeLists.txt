include_directories(${GTEST_INCLUDE_DIRS})

collect_sources(UNITTESTMOCKS)
add_library(unittest-mocks STATIC ${UNITTESTMOCKS_SOURCES})
create_unittest(unittest-mocks ${UNITTESTMOCKS_TESTS})
link_unittest(unittest-mocks unittest-mocks mfscommon)
