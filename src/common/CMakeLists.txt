include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Use crcutil Library
include_directories(${CRCUTIL_INCLUDE_DIRS})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CRCUTIL_CXX_FLAGS}")

collect_sources(COMMON)
add_library(lfscommon ${COMMON_SOURCES})
target_link_libraries(lfscommon crcutil)
if (ZLIB_FOUND)
  target_link_libraries(lfscommon ${ZLIB_LIBRARIES})
endif()
if (UNIX AND NOT APPLE)
  target_link_libraries(lfscommon ${RT_LIBRARY})
endif()
add_tests(lfscommon ${COMMON_TESTS})
