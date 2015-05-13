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
set(PACKAGE_VERSION_MAJOR 2)
set(PACKAGE_VERSION_MINOR 5)
set(PACKAGE_VERSION_MICRO 5)
set(PACKAGE_VERSION
    "${PACKAGE_VERSION_MAJOR}.${PACKAGE_VERSION_MINOR}.${PACKAGE_VERSION_MICRO}")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

set(CXX_STD c++0x)
if(CYGWIN)
  set(CXX_STD gnu++11)
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pipe -std=${CXX_STD} -pthread -Wall -Wextra -fwrapv")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG -g")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O2 -DNDEBUG -g -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_COVERAGE "-O0 -g --coverage")
set(CMAKE_CXX_FLAGS_CLANG_ANALYZE "--analyze -Xanalyzer -analyzer-output=text")
set(CMAKE_CXX_FLAGS_CLANG_SANITIZE_THREAD "-g -O0 -fsanitize=thread")

add_definitions(-D_LARGEFILE_SOURCE)
add_definitions(-D_FILE_OFFSET_BITS=64)
add_definitions(-DLIZARDFS_HAVE_CONFIG_H)
add_definitions(-D__STDC_LIMIT_MACROS)
add_definitions(-D__STDC_CONSTANT_MACROS)
add_definitions(-D__STDC_FORMAT_MACROS)

# XXX(lamvak): what about OS X?
set(DEFAULT_USER  "mfs" CACHE STRING "Default user to run daemons as")
set(DEFAULT_GROUP "mfs" CACHE STRING "Default group to run daemons as")
option(ENABLE_WERROR        "Enable treating compilation warnings as errors"          OFF)
option(ENABLE_LIGHTMFS      "Enable light version of LizardFS"                        OFF)
option(ENABLE_DEBIAN_PATHS  "Enable Debian-style install paths"                       OFF)
option(ENABLE_UTILS         "Enable building additional binaries used e.g. in tests"  OFF)
option(ENABLE_TESTS         "Enable building unit and functional tests"               OFF)
option(ENABLE_DOCS          "Enable building the documentation"                       ON)
option(ENABLE_EXIT_ON_USR1  "Enable handler for SIGUSR1 which calls exit()"           OFF)
option(THROW_INSTEAD_OF_ABORT   "Throw std::exception instead of calling abort"       OFF)
option(ENABLE_DEBUG_LOG     "Enable prefix based debug logs"                          OFF)
option(ENABLE_HA_CLUSTER    "Enable installation of HA scripts"                       ON)
option(ENABLE_VERBOSE_ASCIIDOC  "Enable verbose output of asciidoc"                   OFF)
option(ENABLE_TCMALLOC      "Enable use of tcmalloc-minimal library"                  ON)
# end of LizardFS options

message(STATUS "CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}")
message(STATUS "CMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}")
message(STATUS "DEFAULT_USER=${DEFAULT_USER}")
message(STATUS "DEFAULT_GROUP=${DEFAULT_GROUP}")
message(STATUS "ENABLE_WERROR=${ENABLE_WERROR}")
message(STATUS "ENABLE_LIGHTMFS=${ENABLE_LIGHTMFS}")
message(STATUS "ENABLE_DEBIAN_PATHS=${ENABLE_DEBIAN_PATHS}")
message(STATUS "ENABLE_UTILS=${ENABLE_UTILS}")
message(STATUS "ENABLE_TESTS=${ENABLE_TESTS}")
message(STATUS "ENABLE_DOCS=${ENABLE_DOCS}")
message(STATUS "ENABLE_EXIT_ON_USR1=${ENABLE_EXIT_ON_USR1}")
message(STATUS "THROW_INSTEAD_OF_ABORT=${THROW_INSTEAD_OF_ABORT}")
message(STATUS "ENABLE_DEBUG_LOG=${ENABLE_DEBUG_LOG}")
message(STATUS "ENABLE_HA_CLUSTER=${ENABLE_HA_CLUSTER}")
message(STATUS "ENABLE_VERBOSE_ASCIIDOC=${ENABLE_VERBOSE_ASCIIDOC}")
# end of LizardFS options values

# All the environment tests (libs, includes, etc.) live here:
include(EnvTests.cmake)
include(Libraries)
include(GNUInstallDirs)
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

if(ENABLE_WERROR)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif()

if(ENABLE_TESTS AND NOT THROW_INSTEAD_OF_ABORT)
  message(STATUS "Tests require THROW_INSTEAD_OF_ABORT to be set to YES, changing passed value:")
    set(THROW_INSTEAD_OF_ABORT YES)
  message(STATUS "THROW_INSTEAD_OF_ABORT=${THROW_INSTEAD_OF_ABORT}")
endif()

if(ENABLE_DEBIAN_PATHS)
  if (NOT CMAKE_INSTALL_PREFIX STREQUAL "/")
    message(WARNING "Debian paths enabled, but CMAKE_INSTALL_PREFIX is different than /")
  endif()
  set(BIN_SUBDIR   "usr/bin")
  set(SBIN_SUBDIR  "usr/sbin")
  set(LIB_SUBDIR   "usr/lib")
  set(ETC_SUBDIR   "etc/mfs")
  set(DATA_SUBDIR  "var/lib/mfs")
  set(MAN_SUBDIR   "usr/share/man")
  set(CGI_SUBDIR   "usr/share/mfscgi")
  set(OCF_SUBDIR   "usr/lib/ocf/resource.d/lizardfs")
else()
  set(BIN_SUBDIR   "bin")
  set(SBIN_SUBDIR  "sbin")
  set(LIB_SUBDIR   "lib")
  set(ETC_SUBDIR   "etc/mfs")
  set(DATA_SUBDIR  "var/lib/mfs")
  set(MAN_SUBDIR   "share/man")
  set(CGI_SUBDIR   "share/mfscgi")
  set(OCF_SUBDIR   "lib/ocf/resource.d/lizardfs")
endif()

string(REGEX REPLACE "/$" "" INSTALL_PREFIX_NO_SLASH ${CMAKE_INSTALL_PREFIX})
set(BIN_PATH   ${INSTALL_PREFIX_NO_SLASH}/${BIN_SUBDIR})
set(SBIN_PATH  ${INSTALL_PREFIX_NO_SLASH}/${SBIN_SUBDIR})
set(ETC_PATH   ${INSTALL_PREFIX_NO_SLASH}/${ETC_SUBDIR})
set(DATA_PATH  ${INSTALL_PREFIX_NO_SLASH}/${DATA_SUBDIR})
set(MAN_PATH   ${INSTALL_PREFIX_NO_SLASH}/${MAN_SUBDIR})
set(CGI_PATH   ${INSTALL_PREFIX_NO_SLASH}/${CGI_SUBDIR})
set(OCF_PATH   ${INSTALL_PREFIX_NO_SLASH}/${OCF_SUBDIR})

if(ENABLE_LIGHTMFS)
  set(PROTO_BASE 65536)
  set(LIGHT_MFS 1)
else()
  set(PROTO_BASE 0)
  set(LIGHT_MFS 0)
endif()

if(ENABLE_TESTS)
  set(BUILD_TESTS ON)
  set(BUILD_UTILS ON)
  set(LIZARDFS_ENABLE_DEBUG_LOG ON)
endif()

if(ENABLE_DEBUG_LOG)
  set(LIZARDFS_ENABLE_DEBUG_LOG ON)
endif()

set(CHARTS_CSV_CHARTID_BASE 90000)

if(THROW_INSTEAD_OF_ABORT)
  add_definitions(-DTHROW_INSTEAD_OF_ABORT)
endif()

# Create config.h file
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

if(ZLIB_FOUND)
  include_directories(${ZLIB_INCLUDE_DIRS})
endif()
include_directories(src) # Allow paths relative to src, eg. #include common/crc.h
add_subdirectory(external)
add_subdirectory(src/admin)
add_subdirectory(src/common)
add_subdirectory(src/chunkserver)
add_subdirectory(src/master)
add_subdirectory(src/metadump)
add_subdirectory(src/metalogger)
add_subdirectory(src/metarestore)
add_subdirectory(src/tools)
add_subdirectory(src/mount)
if (FUSE_LIBRARY)
  add_subdirectory(src/mount/fuse)
endif()
add_subdirectory(src/mount/polonaise)
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
if (ENABLE_HA_CLUSTER)
  add_subdirectory(src/ha-cluster)
endif()
