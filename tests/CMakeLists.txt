# Download gtest if not present
if(NOT IS_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/googletest)
  execute_process(
    COMMAND svn checkout http://googletest.googlecode.com/svn/trunk/ googletest
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    RESULT_VARIABLE CHECKOUT_ERROR
    ERROR_VARIABLE  CHECKOUT_ERROR_MESSAGE)
  if(NOT CHECKOUT_ERROR STREQUAL 0)
    message(FATAL_ERROR "svn checkout failed: ${CHECKOUT_ERROR} ${CHECKOUT_ERROR_MESSAGE}")
  endif()
endif()
add_subdirectory(googletest)
include_directories(googletest/include)

add_subdirectory(common)
add_subdirectory(mfschunkserver)
add_subdirectory(mfscommon)
