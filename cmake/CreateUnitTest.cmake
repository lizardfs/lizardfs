function(create_unittest TEST_NAME)
  if(NOT BUILD_TESTS OR ARGC EQUAL 1)
    return()
  endif()
  list(REMOVE_AT ARGV 0)
  set(TEST_LIBRARY_NAME ${TEST_NAME}_unittest)

  include_directories(${GTEST_INCLUDE_DIRS})

  add_library(${TEST_LIBRARY_NAME} ${ARGV})

  list(FIND UNITTEST_TEST_NAMES ${TEST_NAME} result)
  if (${result} EQUAL -1)
    set(TMP ${UNITTEST_TEST_NAMES})
    list(APPEND TMP ${TEST_NAME})
    set(UNITTEST_TEST_NAMES ${TMP} CACHE INTERNAL "" FORCE)
  endif()
endfunction(create_unittest)

function(link_unittest TEST_NAME)
  if(NOT BUILD_TESTS OR ARGC EQUAL 1)
    return()
  endif()
  list(REMOVE_AT ARGV 0)

  set(${TEST_NAME}_UNITTEST_LINKLIST ${ARGV} CACHE INTERNAL "" FORCE)
endfunction(link_unittest)
