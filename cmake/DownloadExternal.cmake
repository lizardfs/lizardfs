function(download_external PCKG_NAME PCKG_DIR_NAME PCKG_URL PCKG_MD5)
  set(${PCKG_NAME}_DIR_NAME ${PCKG_DIR_NAME} CACHE INTERNAL "" FORCE)

  if(NOT IS_DIRECTORY ${CMAKE_SOURCE_DIR}/external/${PCKG_DIR_NAME})
    message(STATUS "Downloading ${PCKG_URL}...")
    if(PCKG_MD5)
      file(DOWNLOAD
          ${PCKG_URL}
          ${CMAKE_BINARY_DIR}/${PCKG_DIR_NAME}.zip
          INACTIVITY_TIMEOUT 15
          SHOW_PROGRESS
          STATUS DOWNLOAD_STATUS
          EXPECTED_MD5 ${PCKG_MD5})
    else()
      file(DOWNLOAD
          ${PCKG_URL}
          ${CMAKE_BINARY_DIR}/${PCKG_DIR_NAME}.zip
          INACTIVITY_TIMEOUT 15
          SHOW_PROGRESS
          STATUS DOWNLOAD_STATUS)
    endif()

    list(GET DOWNLOAD_STATUS 0 DOWNLOAD_CODE)
    if(NOT DOWNLOAD_CODE EQUAL 0)
      list(GET DOWNLOAD_STATUS 1 DOWNLOAD_MESSAGE)
      message(FATAL_ERROR "Download ${PCKG_URL} error ${DOWNLOAD_CODE}: ${DOWNLOAD_MESSAGE}")
    endif()

    message(STATUS "Unpacking ${PCKG_DIR_NAME}.zip ...")
    execute_process(COMMAND unzip -q ${CMAKE_BINARY_DIR}/${PCKG_DIR_NAME}.zip
      WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/external
      RESULT_VARIABLE UNZIP_ERROR
      ERROR_VARIABLE UNZIP_ERROR_MESSAGE)
    if(NOT UNZIP_ERROR STREQUAL 0)
      message(FATAL_ERROR "unzip ${ARCHIVE_NAME} failed: ${UNZIP_ERROR} ${UNZIP_ERROR_MESSAGE}")
    endif()
    if(NOT IS_DIRECTORY ${CMAKE_SOURCE_DIR}/external/${PCKG_DIR_NAME})
      message(FATAL_ERROR "Extracting ${PCKG_DIR_NAME}.zip didn't produce directory '${PCKG_DIR_NAME}'")
    endif()
    message(STATUS "Downloading ${PCKG_NAME} finished successfully")
  else()
    message(STATUS "Found ${PCKG_NAME}")
  endif()
endfunction()
