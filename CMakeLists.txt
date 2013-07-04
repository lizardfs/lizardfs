cmake_minimum_required(VERSION 2.8)
set(CMAKE_LEGACY_CYGWIN_WIN32 0) # Remove when CMake >= 2.8.4 is required

project(lizardfs)

set(CMAKE_BUILD_TYPE Debug)

set(CMAKE_CXX_FLAGS "-std=gnu++0x -Wall -Wextra -Wshadow -pedantic -fwrapv")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")

add_definitions(-D_LARGEFILE_SOURCE)
add_definitions(-D_FILE_OFFSET_BITS=64)
add_definitions(-DHAVE_CONFIG_H)
add_definitions(-D__STDC_LIMIT_MACROS)
add_definitions(-D__STDC_CONSTANT_MACROS)
add_definitions(-D__STDC_FORMAT_MACROS)

set(CMAKE_BINARY_DIR bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
 
function(check_library NAME REQUIRED)
  find_library(${NAME}_LIB ${ARGN})
  if(${NAME}_LIB)
    message(STATUS "Library (${ARGV2}) found in ${${NAME}_LIB}")
  else()
    if (${REQUIRED})
      message(FATAL_ERROR "Library (${ARGV2}) not found")
    else()
      message(STATUS "Library (${ARGV2}) not found")
    endif()
  endif()
endfunction(check_library)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

find_package(ZLIB REQUIRED)

find_package(Threads)
if(NOT CMAKE_THREAD_LIBS_INIT AND NOT CYGWIN)
  # hack for Gentoo where -pthread works, but find_package somehow can't recognize it
  set(CMAKE_THREAD_LIBS_INIT "-pthread")
endif()  

check_library(FUSE False fuse)

add_subdirectory(mfscommon)
add_subdirectory(mfschunkserver)
if (FUSE_LIB)
  add_subdirectory(mfsmount)
endif()
add_subdirectory(mfsmaster)
add_subdirectory(mfsmetadump)
add_subdirectory(mfsmetalogger)
add_subdirectory(mfsmetarestore)
