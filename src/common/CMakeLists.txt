include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Use crcutil Library
include_directories(${CRCUTIL_INCLUDE_DIRS})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CRCUTIL_CXX_FLAGS}")

aux_source_directory(. COMMON_SOURCES)
add_library(mfscommon ${COMMON_SOURCES})
target_link_libraries(mfscommon crcutil)
