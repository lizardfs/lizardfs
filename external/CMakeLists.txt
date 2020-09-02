# crcutil

if(NOT CRCUTIL_FOUND)
  if(HAVE_CRCUTIL)
    include_directories(${CRCUTIL_INCLUDE_DIRS})
    # Option -w disables warnings during CRC compilation
    # We do not plan to fix the external code
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CRCUTIL_CXX_FLAGS} -w")
    file(GLOB CRCUTIL_SOURCES ${CRCUTIL_SOURCE_DIR}/*.cc)
    shared_add_library(crcutil ${CRCUTIL_SOURCES})
  endif()
endif()
