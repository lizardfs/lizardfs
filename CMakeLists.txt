cmake_minimum_required(VERSION 2.8)
set(CMAKE_LEGACY_CYGWIN_WIN32 0) # Remove when CMake >= 2.8.4 is required
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_MODULE_PATH})

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
  message(FATAL_ERROR
    "In-source builds are disabled for this project. This is for your "
    "safety. To build the project, please create a build tree in "
    "a separate directory, for example:\n"
    "     mkdir ${CMAKE_SOURCE_DIR}/build\n"
    "     cd ${CMAKE_SOURCE_DIR}/build\n"
    "     cmake ..\n"
    "Before doing that you may need to clean up after this try by removing "
    "CMakeCache.txt file and CMakeFiles/ directory.\n"
    "If you REALLY need an in-source build, remove this check from "
    "${CMAKE_CURRENT_LIST_FILE} and try again."
  )
endif()

project(lizardfs)
set(PACKAGE_VERSION_MAJOR 4)
set(PACKAGE_VERSION_MINOR 0)
set(PACKAGE_VERSION_MICRO 0)
set(PACKAGE_VERSION
    "${PACKAGE_VERSION_MAJOR}.${PACKAGE_VERSION_MINOR}.${PACKAGE_VERSION_MICRO}")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

set(CXX_STD c++0x)
if(CYGWIN)
  set(CXX_STD gnu++11)
endif()
set(CMAKE_CXX_FLAGS "-std=${CXX_STD} -Wall -Wextra -fwrapv")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -ftree-vectorize -DNDEBUG -g")
set(CMAKE_CXX_FLAGS_COVERAGE "-O0 -g --coverage")
set(CMAKE_CXX_FLAGS_CLANG_ANALYZE "--analyze -Xanalyzer -analyzer-output=text")
set(CMAKE_CXX_FLAGS_CLANG_SANITIZE_THREAD "-g -O0 -fsanitize=thread")

add_definitions(-D_LARGEFILE_SOURCE)
add_definitions(-D_FILE_OFFSET_BITS=64)
add_definitions(-DLIZARDFS_HAVE_CONFIG_H)
add_definitions(-D__STDC_LIMIT_MACROS)
add_definitions(-D__STDC_CONSTANT_MACROS)
add_definitions(-D__STDC_FORMAT_MACROS)

set(DEFAULT_USER  "mfs" CACHE STRING "Default user to run daemons as")
set(DEFAULT_GROUP "mfs" CACHE STRING "Default group to run daemons as")
set(LIZARDFS_BLOCKS_IN_CHUNK 1024 CACHE INT "Number of blocks in one chunk")
set(LIZARDFS_BLOCK_SIZE 65536 CACHE INT "Number of bytes in one block")
option(ENABLE_WERROR        "Enable treating compilation warnings as errors"          OFF)
option(ENABLE_DEBIAN_PATHS  "Enable Debian-style install paths"                       OFF)
option(ENABLE_UTILS         "Enable building additional binaries used e.g. in tests"  OFF)
option(ENABLE_TESTS         "Enable building unit and functional tests"               OFF)
option(ENABLE_DOCS          "Enable building the documentation"                       ON)
option(ENABLE_EXIT_ON_USR1  "Enable handler for SIGUSR1 which calls exit()"           OFF)
option(THROW_INSTEAD_OF_ABORT   "Throw std::exception instead of calling abort"       OFF)
option(ENABLE_DEBUG_LOG     "Enable prefix based debug logs"                          OFF)
# end of LizardFS options
option(ENABLE_TRACES        "Enable traces"                                           OFF)
option(ENABLE_CRC           "Enable checksums"                                        ON)
option(ENABLE_REQUEST_LOG   "Enable logging request times"                            OFF)
option(USE_LEGACY_READ_MESSAGES "Enable sending old type of messages by mount"        OFF)

message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
message(STATUS "CMAKE_INSTALL_PREFIX: ${CMAKE_INSTALL_PREFIX}")
message(STATUS "DEFAULT_USER: ${DEFAULT_USER}")
message(STATUS "DEFAULT_GROUP: ${DEFAULT_GROUP}")
message(STATUS "LIZARDFS_BLOCKS_IN_CHUNK: ${LIZARDFS_BLOCKS_IN_CHUNK}")
message(STATUS "LIZARDFS_BLOCK_SIZE: ${LIZARDFS_BLOCK_SIZE}")
message(STATUS "ENABLE_WERROR: ${ENABLE_WERROR}")
message(STATUS "ENABLE_DEBIAN_PATHS: ${ENABLE_DEBIAN_PATHS}")
message(STATUS "ENABLE_UTILS: ${ENABLE_UTILS}")
message(STATUS "ENABLE_TESTS: ${ENABLE_TESTS}")
message(STATUS "ENABLE_DOCS: ${ENABLE_DOCS}")
message(STATUS "ENABLE_EXIT_ON_USR1: ${ENABLE_EXIT_ON_USR1}")
message(STATUS "THROW_INSTEAD_OF_ABORT: ${THROW_INSTEAD_OF_ABORT}")
message(STATUS "ENABLE_DEBUG_LOG: ${ENABLE_DEBUG_LOG}")
# end of LizardFS options values
message(STATUS "ENABLE_TRACES: ${ENABLE_TRACES}")
message(STATUS "ENABLE_CRC: ${ENABLE_CRC}")
message(STATUS "ENABLE_REQUEST_LOG: ${ENABLE_REQUEST_LOG}")
message(STATUS "USE_LEGACY_READ_MESSAGES: ${USE_LEGACY_READ_MESSAGES}")

# All the environment tests (libs, includes, etc.) live here:
include(EnvTests.cmake)
include(Libraries)
check_includes("${INCLUDES}")

# Disable -pedantic for old gcc because of unfounded
# "no comma at end of enumerator list" warning during crcutil compilation
execute_process(COMMAND ${CMAKE_CXX_COMPILER} -dumpversion OUTPUT_VARIABLE COMPILER_VERSION)
if(ENABLE_WERROR AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND COMPILER_VERSION VERSION_LESS 4.7)
  message(STATUS "Flag '-pedantic' disabled for GCC < 4.7")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pedantic")
endif()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Qunused-arguments")
endif()

if(CXX_HAS_WNOGNU)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-gnu")
endif()

if(ENABLE_WERROR)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif()

if(ENABLE_TESTS AND NOT THROW_INSTEAD_OF_ABORT)
  message(STATUS "Tests require THROW_INSTEAD_OF_ABORT to be set to YES, changing passed value:")
  set(THROW_INSTEAD_OF_ABORT YES)
  message(STATUS "THROW_INSTEAD_OF_ABORT: ${THROW_INSTEAD_OF_ABORT}")
endif()

if(ENABLE_DEBIAN_PATHS)
  if (NOT CMAKE_INSTALL_PREFIX STREQUAL "/")
    message(WARNING "Debian paths enabled, but CMAKE_INSTALL_PREFIX is different than /")
  endif()
  set(BIN_SUBDIR   "usr/bin")
  set(SBIN_SUBDIR  "usr/sbin")
  set(LIB_SUBDIR   "usr/lib")
  set(ETC_SUBDIR   "etc")
  set(RUN_SUBDIR   "var/run/mfs")
  set(DATA_SUBDIR  "var/lib/mfs")
  set(MAN_SUBDIR   "usr/share/man")
  set(CGI_SUBDIR   "usr/share/mfscgi")
else()
  set(BIN_SUBDIR   "bin")
  set(SBIN_SUBDIR  "sbin")
  set(LIB_SUBDIR   "lib")
  set(ETC_SUBDIR   "etc")
  set(RUN_SUBDIR   "var/run/mfs")
  set(DATA_SUBDIR  "var/lib/mfs")
  set(MAN_SUBDIR   "share/man")
  set(CGI_SUBDIR   "share/mfscgi")
endif()
set(BIN_PATH   ${CMAKE_INSTALL_PREFIX}/${BIN_SUBDIR})
set(SBIN_PATH  ${CMAKE_INSTALL_PREFIX}/${SBIN_SUBDIR})
set(LIB_PATH   ${CMAKE_INSTALL_PREFIX}/${LIB_SUBDIR})
set(ETC_PATH   ${CMAKE_INSTALL_PREFIX}/${ETC_SUBDIR})
set(RUN_PATH   ${CMAKE_INSTALL_PREFIX}/${RUN_SUBDIR})
set(DATA_PATH  ${CMAKE_INSTALL_PREFIX}/${DATA_SUBDIR})
set(MAN_PATH   ${CMAKE_INSTALL_PREFIX}/${MAN_SUBDIR})
set(CGI_PATH   ${CMAKE_INSTALL_PREFIX}/${CGI_SUBDIR})

if(ENABLE_UTILS)
  set(BUILD_UTILS ON)
endif()

if(ENABLE_TESTS)
  set(BUILD_TESTS ON)
  set(BUILD_UTILS ON)
  set(LIZARDFS_ENABLE_DEBUG_LOG ON)
endif()

if(ENABLE_DEBUG_LOG)
  set(LIZARDFS_ENABLE_DEBUG_LOG ON)
endif()

if(ENABLE_TRACES)
  if(NOT Boost_INCLUDE_DIRS)
    message(FATAL_ERROR "Boost headers are required if traces are enabled")
  endif()
  add_definitions(-DENABLE_TRACES)
endif()

if(ENABLE_REQUEST_LOG)
  if(NOT Boost_IOSTREAMS_FOUND)
    message(FATAL_ERROR "Boost iostreams is required if request time logging is enabled")
  endif()
  set(ADDITIONAL_LIBS ${Boost_IOSTREAMS_LIBRARY})
  add_definitions(-DENABLE_REQUEST_LOG)
endif()

set(CHARTS_CSV_CHARTID_BASE 90000)

if(THROW_INSTEAD_OF_ABORT)
  add_definitions(-DTHROW_INSTEAD_OF_ABORT)
endif()

if(USE_LEGACY_READ_MESSAGES)
  add_definitions(-DUSE_LEGACY_READ_MESSAGES)
endif()

if(ENABLE_CRC)
  message(STATUS "CRC checksums enabled")
  add_definitions(-DENABLE_CRC)
else()
  message(STATUS "CRC checksums disabled")
endif()

set(CHARTS_CSV_CHARTID_BASE 90000)

# Create config.h file
set(DATA_PATH "${CMAKE_INSTALL_PREFIX}/${DATA_SUBDIR}")
set(ETC_PATH "${CMAKE_INSTALL_PREFIX}/${ETC_SUBDIR}")
set(RUN_PATH "${CMAKE_INSTALL_PREFIX}/${RUN_SUBDIR}")
set(PROTO_BASE 0)
configure_file(config.h.in config.h)
include_directories(${CMAKE_BINARY_DIR}) # Here we have generated config.h

# main.cc shared by some LizardFS applications
set(MAIN_SRC ${CMAKE_SOURCE_DIR}/src/main/main.cc)

# functions & macros
macro(collect_sources VAR_PREFIX)
  file(GLOB ${VAR_PREFIX}_TESTS *_unittest.cc)
  file(GLOB ${VAR_PREFIX}_SOURCES *.cc *.h)
  file(GLOB ${VAR_PREFIX}_MAIN main.cc)
  if(${VAR_PREFIX}_MAIN OR ${VAR_PREFIX}_TESTS)
    list(REMOVE_ITEM ${VAR_PREFIX}_SOURCES ${${VAR_PREFIX}_TESTS} ${${VAR_PREFIX}_MAIN})
  endif()
endmacro(collect_sources)

function(add_tests TESTED_LIBRARY)
  if(NOT BUILD_TESTS OR ARGC EQUAL 1)
    return()
  endif()
  list(REMOVE_AT ARGV 0)
  set(TEST_LIBRARY_NAME ${TESTED_LIBRARY}_tests)
  include_directories(${GTEST_INCLUDE_DIRS})
  add_library(${TEST_LIBRARY_NAME} ${ARGV})
  target_link_libraries(${TEST_LIBRARY_NAME} ${TESTED_LIBRARY})
  set(TMP ${TEST_LIBRARIES})
  list(APPEND TMP ${TEST_LIBRARY_NAME})
  set(TEST_LIBRARIES ${TMP} CACHE INTERNAL "" FORCE)
endfunction(add_tests)

include_directories(${Boost_INCLUDE_DIRS})
include_directories(src) # Allow paths relative to src, eg. #include common/crc.h
add_subdirectory(external)
add_subdirectory(src/common)
if(BUILD_DEVTOOLS)
  add_subdirectory(src/devtools)
endif()
add_subdirectory(src/chunkserver)
add_subdirectory(src/master)
add_subdirectory(src/metadump)
add_subdirectory(src/metalogger)
add_subdirectory(src/metarestore)
add_subdirectory(src/probe)
add_subdirectory(src/tools)
add_subdirectory(src/mount)
if (FUSE_LIBRARY)
  add_subdirectory(src/mount/fuse)
endif()
if(POLONAISE_FOUND)
  add_subdirectory(src/mount/polonaise)
endif()
add_subdirectory(src/data)
add_subdirectory(src/cgi)
if(BUILD_UTILS)
  add_subdirectory(utils)
endif()
if(BUILD_TESTS)
  add_subdirectory(src/unittests)
  add_subdirectory(tests)
endif()
if(ENABLE_DOCS)
  add_subdirectory(doc)
endif()

include(Packages.cmake)
