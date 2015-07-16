include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Use crcutil Library
include_directories(${CRCUTIL_INCLUDE_DIRS})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CRCUTIL_CXX_FLAGS}")

collect_sources(COMMON)

if(MINGW)
list(REMOVE_ITEM COMMON_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/lockfile.cc")
list(REMOVE_ITEM COMMON_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/metadata.cc")
list(REMOVE_ITEM COMMON_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/setup.cc")
endif()

add_library(mfscommon ${COMMON_SOURCES})
target_link_libraries(mfscommon crcutil)
if (ZLIB_FOUND)
  target_link_libraries(mfscommon ${ZLIB_LIBRARIES})
endif()
if (RT_LIBRARY)
  target_link_libraries(mfscommon ${RT_LIBRARY})
endif()
if(SOCKET_FOUND)
  target_link_libraries(mfscommon ${SOCKET_LIBRARIES})
endif()
add_tests(mfscommon ${COMMON_TESTS})
