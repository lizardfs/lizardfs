cmake_minimum_required(VERSION 2.8)
set(CMAKE_LEGACY_CYGWIN_WIN32 0) # Remove when CMake >= 2.8.4 is required

#INCLUDE (CheckIncludeFiles)
#INCLUDE (CheckLibraryExists)
INCLUDE(TestBigEndian)

project(lizardfs)
set(PACKAGE_VERSION_MAJOR 1)
set(PACKAGE_VERSION_MINOR 6)
set(PACKAGE_VERSION_MICRO 27)

set(CMAKE_BUILD_TYPE Debug)

set(CMAKE_CXX_FLAGS "-std=gnu++0x -Wall -Wextra -Wshadow -pedantic -fwrapv")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")

add_definitions(-D_LARGEFILE_SOURCE)
add_definitions(-D_FILE_OFFSET_BITS=64)
add_definitions(-DHAVE_CONFIG_H)
add_definitions(-D__STDC_LIMIT_MACROS)
add_definitions(-D__STDC_CONSTANT_MACROS)
add_definitions(-D__STDC_FORMAT_MACROS)

#set(CMAKE_BINARY_DIR bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
# XXX(lamvak): what about windows/OS X?
set(DATA_PATH "${CMAKE_INSTALL_PREFIX}/var/mfs" CACHE PATH "Data directory")
set(ETC_PATH "${CMAKE_INSTALL_PREFIX}/etc/mfs" CACHE PATH "System configuration directory for mfs")
set(RUN_PATH "${CMAKE_INSTALL_PREFIX}/var/run/mfs" CACHE PATH "PID file"
	" directory")

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_MODULE_PATH})
 
function(check_library NAME REQUIRED)
  find_library(${NAME}_LIB ${ARGN})
  if(${NAME}_LIB)
    message(STATUS "Library ${ARGV2} found in ${${NAME}_LIB}")
  else()
    if (${REQUIRED})
      message(FATAL_ERROR "Library ${ARGV2} not found")
    else()
      message(STATUS "Library ${ARGV2} not found")
    endif()
  endif()
endfunction(check_library)

find_package(ZLIB REQUIRED)
find_package(Threads)
#	find_package(Threads)
#	if(NOT CMAKE_THREAD_LIBS_INIT AND NOT CYGWIN)
#	  # hack for Gentoo where -pthread works, but find_package somehow can't recognize it
#	  set(CMAKE_THREAD_LIBS_INIT "-pthread")
#	endif()  

check_library(FUSE False fuse)
#check_library_exists(socket socket "" HAVE_LIBSOCKET)
find_package(Socket)
check_library_exists(nsl getaddrinfo "" HAVE_NSL)
check_library_exists(m round "" HAVE_ROUND)

message(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_BINARY_DIR})

set(ADDITIONAL_INCLUDES_FOUND TRUE)
set(ADDITIONAL_INCLUDES arpa/inet.h fcntl.h inttypes.h limits.h netdb.h
	netinet/in.h stddef.h stdlib.h string.h sys/socket.h sys/statvfs.h
	sys/time.h syslog.h unistd.h stdbool.h)
foreach(INCLUDE_FILE ${ADDITIONAL_INCLUDES})
	CHECK_INCLUDE_FILES(${INCLUDE_FILE} ${INCLUDE_FILE}_FOUND)
	if(NOT ${INCLUDE_FILE}_FOUND)
		set(ADDITIONAL_INCLUDES_FOUND FALSE)
		set(MISSING_INCLUDES ${MISSING_INCLUDES} ${INCLUDE_FILE})
	endif()
endforeach()
if(NOT ADDITIONAL_INCLUDES_FOUND)
	message(FATAL_ERROR "Some include files were not found: ${MISSING_INCLUDES}")
endif()

#XXX(lamvak): convert AC_C_CONST check
TEST_BIG_ENDIAN(BIG_ENDIAN)
if(BIG_ENDIAN)
	set(WORDS_BIGENDIAN)
endif()
include(CMakeTestInline)
#XXX(lamvak): convert AC_C_VOLATILE check; BUT, is it really OK to discard
# volatile in LFS
# stdbool.h tested with other headers above

set(ENABLE_LIGHTMFS FALSE CACHE BOOL "Enable light version of LizardFS")
get_property(CACHE_ENABLE_LIGHTMFS_DEFINED CACHE ENABLE_LIGHTMFS PROPERTY VALUE
	DEFINED)
if(ENABLE_LIGHTMFS)
	message("light mfs: ${CACHE_ENABLE_LIGHTMFS_VALUE}")
	set(PROTO_BASE 65536)
	set(LIGHT_MFS 1)
else()
	message("no light mfs!")
	set(PROTO_BASE 0)
	set(LIGHT_MFS 0)
endif()

set(DEFAULT_USER "nobody" CACHE STRING "Default user to run daemons as")
set(DEFAULT_GROUP "" CACHE STRING "Default group to run daemons as")

configure_file(${CMAKE_SOURCE_DIR}/config.h.in ${CMAKE_BINARY_DIR}/config.h)

add_subdirectory(mfscommon)
add_subdirectory(mfschunkserver)
if (FUSE_LIB)
  add_subdirectory(mfsmount)
endif()
add_subdirectory(mfsmaster)
add_subdirectory(mfsmetadump)
add_subdirectory(mfsmetalogger)
add_subdirectory(mfsmetarestore)
