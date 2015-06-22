# crcutil

if(NOT CRCUTIL_FOUND)
  if(HAVE_CRCUTIL)
    include_directories(${CRCUTIL_INCLUDE_DIRS})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CRCUTIL_CXX_FLAGS}")
    file(GLOB CRCUTIL_SOURCES ${CRCUTIL_SOURCE_DIR}/*.cc)
    add_library(crcutil ${CRCUTIL_SOURCES})
  endif()
endif()

# GoogleTest
if(BUILD_TESTS)
  include_directories(${GTEST_INCLUDE_DIRS})
  add_subdirectory(${GTEST_NAME})
endif()
