# Download crcutil
set(CRCUTIL_VERSION crcutil-1.0)
set(CRCUTIL_ARCHIVE ${CRCUTIL_VERSION}.tar.gz)
set(CRCUTIL_URL http://crcutil.googlecode.com/files/${CRCUTIL_ARCHIVE})
set(CRCUTIL_MD5 94cb7014d4078c138d3c9646fcf1fec5)
if(NOT IS_DIRECTORY ${CMAKE_SOURCE_DIR}/external/${CRCUTIL_VERSION})
  message(STATUS "Downloading ${CRCUTIL_URL} ...")
  file(DOWNLOAD
      ${CRCUTIL_URL}
      ${CMAKE_BINARY_DIR}/${CRCUTIL_ARCHIVE}
      INACTIVITY_TIMEOUT 15
      STATUS DOWNLOAD_STATUS
      EXPECTED_MD5 ${CRCUTIL_MD5}
      SHOW_PROGRESS)
  list(GET DOWNLOAD_STATUS 0 DOWNLOAD_STATUS_ERROR_CODE)
  list(GET DOWNLOAD_STATUS 1 DOWNLOAD_STATUS_MESSAGE)
  if (NOT DOWNLOAD_STATUS_ERROR_CODE EQUAL 0)
    message(FATAL_ERROR "Downloading failed: ${DOWNLOAD_STATUS_MESSAGE}")
  endif()
  message(STATUS "Unpacking ${CRCUTIL_ARCHIVE} ...")
  execute_process(COMMAND tar -xzf ${CMAKE_BINARY_DIR}/${CRCUTIL_ARCHIVE}
      WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/external
      RESULT_VARIABLE TAR_ERROR
      ERROR_VARIABLE  TAR_ERROR_MESSAGE)
  if(NOT TAR_ERROR STREQUAL 0)
    message(FATAL_ERROR "tar -xzf ${CRCUTIL_ARCHIVE} failed: ${TAR_ERROR_MESSAGE}")
  endif()
  if(NOT IS_DIRECTORY ${CMAKE_SOURCE_DIR}/external/${CRCUTIL_VERSION})
    message(FATAL_ERROR "Extracting ${CRCUTIL_ARCHIVE} didn't produce directory ${CRCUTIL_VERSION}")
  endif()
endif()

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
find_package(Threads)
find_package(ZLIB REQUIRED)
find_package(Boost COMPONENTS filesystem system regex thread)
find_library(FUSE_LIBRARY fuse)
message(STATUS "FUSE_LIBRARY: ${FUSE_LIBRARY}")

# Find crcutil
set(CRCUTIL_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/external/${CRCUTIL_VERSION}/code)
set(CRCUTIL_SOURCE_DIR ${CMAKE_SOURCE_DIR}/external/${CRCUTIL_VERSION}/code)
set(CRCUTIL_CXX_FLAGS "-mcrc32")

# Find GoogleTest
if(ENABLE_TESTS)
  set(GTEST_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/external/${GTEST_NAME}/include)
  set(TEST_LIBRARIES "" CACHE INTERNAL "" FORCE)
endif()
