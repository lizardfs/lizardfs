# Download GoogleTest
if(ENABLE_TESTS)
  set(GTEST_VERSION 1.7.0)
  set(GTEST_NAME googletest-release-${GTEST_VERSION})

  if(NOT IS_DIRECTORY ${CMAKE_SOURCE_DIR}/external/${GTEST_NAME})
    set(GTEST_ARCHIVE release-${GTEST_VERSION}.zip)
    set(GTEST_URL https://github.com/google/googletest/archive/${GTEST_ARCHIVE})
    set(GTEST_ARCHIVE_MD5 ef5e700c8a0f3ee123e2e0209b8b4961)

    message(STATUS "Downloading ${GTEST_URL}...")
    file(DOWNLOAD
        ${GTEST_URL}
        ${CMAKE_BINARY_DIR}/${GTEST_ARCHIVE}
        INACTIVITY_TIMEOUT 15
        SHOW_PROGRESS
        STATUS DOWNLOAD_STATUS
        EXPECTED_MD5 ${GTEST_ARCHIVE_MD5})

    list(GET DOWNLOAD_STATUS 0 DOWNLOAD_CODE)
    if(NOT DOWNLOAD_CODE EQUAL 0)
      list(GET DOWNLOAD_STATUS 1 DOWNLOAD_MESSAGE)
      message(FATAL_ERROR "Download ${GTEST_URL} error ${DOWNLOAD_CODE}: ${DOWNLOAD_MESSAGE}")
    endif()

    message(STATUS "Unpacking ${GTEST_ARCHIVE}...")
    execute_process(COMMAND unzip -q ${CMAKE_BINARY_DIR}/${GTEST_ARCHIVE}
      WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/external
      RESULT_VARIABLE UNZIP_ERROR
      ERROR_VARIABLE UNZIP_ERROR_MESSAGE)
    if(NOT UNZIP_ERROR STREQUAL 0)
    message(FATAL_ERROR "unzip ${GTEST_ARCHIVE} failed: ${UNZIP_ERROR} ${UNZIP_ERROR_MESSAGE}")
    endif()
    if(NOT IS_DIRECTORY ${CMAKE_SOURCE_DIR}/external/${GTEST_NAME})
      message(FATAL_ERROR "Extracting ${GTEST_ARCHIVE} didn't produce directory '${GTEST_NAME}'")
    endif()
    message(STATUS "Downloading ${GTEST_NAME} finished successfully")
  else()
    message(STATUS "Found ${GTEST_NAME}")
  endif()
endif()

# Find standard libraries
find_package(Socket REQUIRED)
find_package(Threads REQUIRED)

if(NOT MINGW)
  find_package(FUSE REQUIRED)
endif()

find_library(RT_LIBRARY rt)
message(STATUS "RT_LIBRARY: ${RT_LIBRARY}")
if(ENABLE_TCMALLOC)
  find_library(TCMALLOC_LIBRARY NAMES tcmalloc_minimal)
  message(STATUS "TCMALLOC_LIBRARY: ${TCMALLOC_LIBRARY}")
endif()


# Find extra binaries
find_program(A2X_BINARY a2x)
message(STATUS "a2x: ${A2X_BINARY}")

# Find Zlib
find_package(ZLIB)
if(ZLIB_FOUND)
  message(STATUS "Found Zlib ${ZLIB_VERSION_STRING}")
  set(LIZARDFS_HAVE_ZLIB_H 1)
else()
  message(STATUS "Could not find Zlib")
  message(STATUS "   This dependency is optional.")
  message(STATUS "   If it's installed in a non-standard path, set ZLIB_ROOT variable")
  message(STATUS "   to point this path (cmake -DZLIB_ROOT=...)")
endif()

# Find Systemd
INCLUDE(FindPkgConfig)
pkg_check_modules(SYSTEMD libsystemd)
if(SYSTEMD_FOUND)
  check_include_files(systemd/sd-daemon.h LIZARDFS_HAVE_SYSTEMD_SD_DAEMON_H)
  message(STATUS "Found Systemd ${SYSTEMD_VERSION_STRING}")
else()
  message(STATUS "Could not find Systemd (but it is not required)")
endif()

# Find Boost
set(BOOST_MIN_VERSION "1.48.0")
find_package(Boost ${BOOST_MIN_VERSION} COMPONENTS filesystem iostreams program_options system)

# Find Thrift
find_package(Thrift COMPONENTS library)
if(THRIFT_FOUND)
  message(STATUS "Found Thrift")
else()
  message(STATUS "Could NOT find Thrift (but it's not required)")
  message(STATUS "   If it's installed in a non-standard path, set THRIFT_ROOT variable")
  message(STATUS "   to point this path (cmake -DTHRIFT_ROOT=...)")
endif()

# Find Polonaise
set(POLONAISE_REQUIRED_VERSION 0.3.1)
find_package(Polonaise ${POLONAISE_REQUIRED_VERSION} EXACT QUIET NO_MODULE NO_CMAKE_BUILDS_PATH)
if(POLONAISE_FOUND)
  message(STATUS "Found Polonaise")
else()
  message(STATUS "Could NOT find Polonaise v${POLONAISE_REQUIRED_VERSION} (but it's not required)")
  if(Polonaise_CONSIDERED_VERSIONS)
    message(STATUS "   Incompatible versions ${Polonaise_CONSIDERED_VERSIONS} "
        "found in ${Polonaise_CONSIDERED_CONFIGS}")
  endif()
  message(STATUS "   If it's installed in a non-standard path, set Polonaise_DIR variable")
  message(STATUS "   to point this path (cmake -DPolonaise_DIR=...)")
endif()

# Find crcutil
if(NOT BIG_ENDIAN)
  INCLUDE(FindPkgConfig)
  pkg_check_modules(CRCUTIL libcrcutil)
  if(CRCUTIL_FOUND)
    message(STATUS "Found libcrcutil")
    set(HAVE_CRCUTIL 1)
  else()
    message(STATUS "Could NOT find system libcrcutil (but it's not required)")
    set(CRCUTIL_VERSION crcutil-1.0)
    message(STATUS "Using bundled ${CRCUTIL_VERSION}")
    set(HAVE_CRCUTIL 1)
    set(CRCUTIL_LIBRARIES "crcutil")
    set(CRCUTIL_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/external/${CRCUTIL_VERSION}/code)
    set(CRCUTIL_SOURCE_DIR ${CMAKE_SOURCE_DIR}/external/${CRCUTIL_VERSION}/code)

    if(CXX_HAS_MCRC32)
      set(CRCUTIL_CXX_FLAGS "-mcrc32")
    else()
      set(CRCUTIL_CXX_FLAGS "")
    endif()

  endif()
endif()

# Find GoogleTest
if(ENABLE_TESTS)
  set(GTEST_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/external/${GTEST_NAME}/include)
  set(TEST_LIBRARIES "" CACHE INTERNAL "" FORCE)
endif()

# Find Judy
find_package(Judy)
if(JUDY_FOUND)
  set(LIZARDFS_HAVE_JUDY YES)
endif()

# Find PAM libraries
find_package(PAM)
if(PAM_FOUND)
  set(LIZARDFS_HAVE_PAM YES)
endif()

# Find BerkeleyDB
find_package(DB 11.2.5.2)

# Find Intel Storage Acceleration library
find_library(ISAL_LIBRARY isal)
message(STATUS "ISAL(Intel Storage Acceleration) LIBRARY: ${ISAL_LIBRARY}")

