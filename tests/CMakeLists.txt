include_directories(${GTEST_INCLUDE_DIRS})

# Create automatically generated list of test cases

if ( NOT ( ENABLE_POLONAISE AND POLONAISE_FOUND ) )
  set ( filter_out "|" "grep" "-v" "polonaise" )
endif ()
add_custom_target(list_of_test_cases
  COMMAND ls ${CMAKE_CURRENT_SOURCE_DIR}/test_suites
    | xargs -IXXX echo "class XXX : public BashTestingSuite {};"
    > test_suites.h.tmp
  COMMAND ${CMAKE_COMMAND} -E copy_if_different test_suites.h.tmp test_suites.h

  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR} && bash tools/generate_tests_from_templates.sh

  COMMAND find ${CMAKE_CURRENT_SOURCE_DIR}/test_suites/ -name test_*.sh
    ${filter_out}
    | sed -e "s/[.]sh$//"
    | awk -F/ "{print $(NF-1) \",\" $NF}"
    | xargs -IXXX echo "add_test_case(XXX)"
    > test_cases.h.tmp
  COMMAND ${CMAKE_COMMAND} -E copy_if_different test_cases.h.tmp test_cases.h

  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  VERBATIM)
set_source_files_properties(lizardfs-tests.cc PROPERTIES
  OBJECT_DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/test_cases.h
  OBJECT_DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/test_suites.h)

# Create constants.sh file
configure_file(set_lizardfs_constants.sh.in set_lizardfs_constants.sh @ONLY)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/set_lizardfs_constants.sh DESTINATION ${BIN_SUBDIR})

# Build executable running all the test cases
include_directories(${CMAKE_CURRENT_BINARY_DIR}) # test_cases.h is located here
add_definitions(-DTEST_DATA_PATH=${CMAKE_CURRENT_SOURCE_DIR})
add_executable(lizardfs-tests lizardfs-tests.cc)
add_dependencies(lizardfs-tests list_of_test_cases)
target_link_libraries(lizardfs-tests ${GTEST_BOTH_LIBRARIES} ${Boost_SYSTEM_LIBRARY} ${Boost_FILESYSTEM_LIBRARY})
install(TARGETS lizardfs-tests RUNTIME DESTINATION ${BIN_SUBDIR})
