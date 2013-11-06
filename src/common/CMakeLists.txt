include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Use crcutil Library
include_directories(${CRCUTIL_INCLUDE_DIRS})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CRCUTIL_CXX_FLAGS}")

collect_sources(COMMON)
add_library(mfscommon ${COMMON_SOURCES})
target_link_libraries(mfscommon crcutil)
if (UNIX AND NOT APPLE)
  target_link_libraries(mfscommon ${RT_LIBRARY})
endif()
add_tests(mfscommon ${COMMON_TESTS})
