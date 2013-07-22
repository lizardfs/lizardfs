cmake_minimum_required(VERSION 2.8)
set(CMAKE_LEGACY_CYGWIN_WIN32 0) # Remove when CMake >= 2.8.4 is required
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_MODULE_PATH})

include(TestBigEndian)

project(lizardfs)
set(PACKAGE_VERSION_MAJOR 1)
set(PACKAGE_VERSION_MINOR 6)
set(PACKAGE_VERSION_MICRO 27)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

set(CMAKE_CXX_FLAGS "-std=gnu++0x -Wall -Wextra -Wshadow -pedantic -fwrapv")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

add_definitions(-D_LARGEFILE_SOURCE)
add_definitions(-D_FILE_OFFSET_BITS=64)
add_definitions(-DHAVE_CONFIG_H)
add_definitions(-D__STDC_LIMIT_MACROS)
add_definitions(-D__STDC_CONSTANT_MACROS)
add_definitions(-D__STDC_FORMAT_MACROS)

# XXX(lamvak): what about windows/OS X?
set(ETC_SUBDIR "etc" CACHE STRING "System config directory for mfs")
set(DATA_SUBDIR "var" CACHE STRING "Data directory")
set(RUN_SUBDIR "var/run" CACHE STRING "PID file directory")
set(MAN_SUBDIR "share/man" CACHE STRING "Manpages direcory")
set(DEFAULT_USER "nobody" CACHE STRING "Default user to run daemons as")
set(DEFAULT_GROUP "" CACHE STRING "Default group to run daemons as")
set(ENABLE_LIGHTMFS NO CACHE STRING "Enable light version of LizardFS")

message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
message(STATUS "CMAKE_INSTALL_PREFIX: ${CMAKE_INSTALL_PREFIX}")
message(STATUS "ENABLE_LIGHTMFS: ${ENABLE_LIGHTMFS}")

find_library(FUSE_LIBRARY fuse)
find_package(ZLIB REQUIRED)
find_package(Socket)
find_package(Threads)
#	find_package(Threads)
#	if(NOT CMAKE_THREAD_LIBS_INIT AND NOT CYGWIN)
#	  # hack for Gentoo where -pthread works, but find_package somehow can't recognize it
#	  set(CMAKE_THREAD_LIBS_INIT "-pthread")
#	endif()  

set(ADDITIONAL_INCLUDES_FOUND TRUE)
set(ADDITIONAL_INCLUDES arpa/inet.h fcntl.h inttypes.h limits.h netdb.h
    netinet/in.h stddef.h stdlib.h string.h sys/socket.h sys/statvfs.h
    sys/time.h syslog.h unistd.h stdbool.h)
foreach(INCLUDE_FILE ${ADDITIONAL_INCLUDES})
  check_include_files(${INCLUDE_FILE} ${INCLUDE_FILE}_FOUND)
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

if(ENABLE_LIGHTMFS)
  set(PROTO_BASE 65536)
  set(LIGHT_MFS 1)
else()
  set(PROTO_BASE 0)
  set(LIGHT_MFS 0)
endif()

# Create config.h file
set(DATA_PATH "${CMAKE_INSTALL_PREFIX}/${DATA_SUBDIR}")
set(ETC_PATH "${CMAKE_INSTALL_PREFIX}/${ETC_SUBDIR}")
set(RUN_PATH "${CMAKE_INSTALL_PREFIX}/${RUN_SUBDIR}")
configure_file(config.h.in config.h)
include_directories(${CMAKE_BINARY_DIR}) # Here we have generated config.h

# main.cc shared by some LizardFS applications
set(MAIN_SRC ${CMAKE_SOURCE_DIR}/mfsmain/main.cc)

add_subdirectory(mfscommon)
add_subdirectory(mfschunkserver)
add_subdirectory(mfsmaster)
add_subdirectory(mfsmetadump)
add_subdirectory(mfsmetalogger)
add_subdirectory(mfsmetarestore)
add_subdirectory(mfstools)
if (FUSE_LIBRARY)
  add_subdirectory(mfsmount)
endif()
add_subdirectory(mfsdata)
add_subdirectory(doc)
