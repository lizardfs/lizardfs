include_directories(${GTEST_INCLUDE_DIRS})

add_subdirectory(mocks)

collect_sources(UNITTESTS)
add_library(unittests-helpers STATIC ${UNITTESTS_SOURCES})

if(APPLE)
  set(ENABLE_WHOLE_ARCHIVE "-Wl,-force_load")
  set(DISABLE_WHOLE_ARCHIVE "")
else()
  set(ENABLE_WHOLE_ARCHIVE "-Wl,--whole-archive")
  set(DISABLE_WHOLE_ARCHIVE "-Wl,--no-whole-archive")
endif()

foreach(unittest_name ${UNITTEST_TEST_NAMES})
  add_executable(unittest_${unittest_name} ${UNITTESTS_MAIN})
  target_link_libraries(unittest_${unittest_name} gtest
        ${ENABLE_WHOLE_ARCHIVE} ${unittest_name}_unittest ${DISABLE_WHOLE_ARCHIVE} ${${unittest_name}_UNITTEST_LINKLIST}
        unittests-helpers unittest-mocks ${Boost_SYSTEM_LIBRARY} ${Boost_FILESYSTEM_LIBRARY})
endforeach()

configure_file(unittests.in unittests @ONLY)
