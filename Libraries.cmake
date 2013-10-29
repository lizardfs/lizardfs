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

# Find standard libraries
find_package(Socket)
find_package(Threads)
find_package(ZLIB REQUIRED)
find_library(FUSE_LIBRARY fuse)
message(STATUS "FUSE_LIBRARY: ${FUSE_LIBRARY}")

# Find crcutil
set(CRCUTIL_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/external/${CRCUTIL_VERSION}/code)
set(CRCUTIL_SOURCE_DIR ${CMAKE_SOURCE_DIR}/external/${CRCUTIL_VERSION}/code)
set(CRCUTIL_CXX_FLAGS "-mcrc32")
