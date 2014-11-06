# Download GoogleTest
if(ENABLE_TESTS)
  set(GTEST_VERSION 1.7.0)
  set(GTEST_NAME gtest-${GTEST_VERSION})

  if(NOT IS_DIRECTORY ${CMAKE_SOURCE_DIR}/external/${GTEST_NAME})
    set(GTEST_ARCHIVE ${GTEST_NAME}.zip)
    set(GTEST_URL http://googletest.googlecode.com/files/${GTEST_ARCHIVE})
    set(GTEST_ARCHIVE_MD5 2d6ec8ccdf5c46b05ba54a9fd1d130d7)

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
find_package(Socket)
find_package(Threads REQUIRED)
find_library(FUSE_LIBRARY fuse)
message(STATUS "FUSE_LIBRARY: ${FUSE_LIBRARY}")
find_library(RT_LIBRARY rt)
message(STATUS "RT_LIBRARY: ${RT_LIBRARY}")

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

# Find Boost
set(BOOST_MIN_VERSION "1.48.0")
find_package(Boost ${BOOST_MIN_VERSION} COMPONENTS program_options)

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
set(CRCUTIL_VERSION crcutil-1.0)
set(CRCUTIL_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/external/${CRCUTIL_VERSION}/code)
set(CRCUTIL_SOURCE_DIR ${CMAKE_SOURCE_DIR}/external/${CRCUTIL_VERSION}/code)

if(CXX_HAS_MCRC32)
    set(CRCUTIL_CXX_FLAGS "-mcrc32")
else()
    set(CRCUTIL_CXX_FLAGS "")
endif()

# Find GoogleTest
if(ENABLE_TESTS)
  set(GTEST_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/external/${GTEST_NAME}/include)
  set(TEST_LIBRARIES "" CACHE INTERNAL "" FORCE)
endif()
