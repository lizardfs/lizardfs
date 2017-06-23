# crcutil

if(NOT CRCUTIL_FOUND)
  if(HAVE_CRCUTIL)
    include_directories(${CRCUTIL_INCLUDE_DIRS})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CRCUTIL_CXX_FLAGS}")
    file(GLOB CRCUTIL_SOURCES ${CRCUTIL_SOURCE_DIR}/*.cc)
    shared_add_library(crcutil ${CRCUTIL_SOURCES})
  endif()
endif()

# GoogleTest
if(BUILD_TESTS)
  if(ENABLE_WERROR)
    set(LIZARDFS_BACKUP_CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers")
  endif()
  include_directories(${GTEST_INCLUDE_DIRS})
  add_subdirectory(${GTEST_DIR_NAME})
  if(ENABLE_WERROR)
    set(CMAKE_CXX_FLAGS "${LIZARDFS_BACKUP_CMAKE_CXX_FLAGS}")
    unset(LIZARDFS_BACKUP_CMAKE_CXX_FLAGS)
  endif()
endif()
