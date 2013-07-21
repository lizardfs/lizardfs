cmake_minimum_required(VERSION 2.8)
project(lizardfs)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

set(CMAKE_CXX_FLAGS "-std=c++0x -Wall -Wextra -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DHAVE_CONFIG_H -pthread")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -ggdb")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

function(find_required_library NAME)
  find_library(${NAME}_LIB ${ARGN})
  if(${NAME}_LIB)
    message(STATUS "Library (${ARGV1}) found in ${${NAME}_LIB}")
  else()
    message(FATAL_ERROR "Library (${ARGV1}) not found")
  endif()
endfunction(find_required_library)

find_required_library(M libm.a)
find_required_library(Z libz.a)
find_required_library(FUSE fuse)
find_required_library(DL dl)

include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/cmake_config)
set(MAIN_CC ${CMAKE_SOURCE_DIR}/mfsdaemonmain/main.cc)

add_subdirectory(mfscommon)
add_subdirectory(mfschunkserver)
add_subdirectory(mfsmaster)
add_subdirectory(mfsmetalogger)
add_subdirectory(mfsmount)
add_subdirectory(mfstools)

