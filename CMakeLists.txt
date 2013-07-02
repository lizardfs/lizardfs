cmake_minimum_required(VERSION 2.8)
set(CMAKE_LEGACY_CYGWIN_WIN32 0) # Remove when CMake >= 2.8.4 is required

project(lizardfs)

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -Wall -Wextra -DHAVE_INTTYPES_H -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DHAVE_CONFIG_H")

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

check_library(M    True  libm.a)
check_library(Z    True  libz.a)
check_library(DL   True  dl)
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