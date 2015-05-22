include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../master)
add_definitions(-DMETARESTORE)
add_definitions(-DAPPNAME=mfsmetarestore)

collect_sources(METARESTORE)

file(GLOB METARESTORE_MASTER_SOURCES ../master/filesystem*.cc)

add_library(metarestore ${METARESTORE_SOURCES} ${METARESTORE_MASTER_SOURCES} ../master/chunks.cc ../master/quota_database.cc ../master/chunk_goal_counters.cc ../master/restore.cc)
target_link_libraries(metarestore mfscommon)
if(JUDY_LIBRARY)
  target_link_libraries(metarestore ${JUDY_LIBRARY})
endif()
add_tests(metarestore ${METARESTORE_TESTS})

add_executable(mfsmetarestore ${METARESTORE_MAIN})
target_link_libraries(mfsmetarestore metarestore)
target_link_libraries(mfsmetarestore mfscommon)
target_link_libraries(mfsmetarestore)
install(TARGETS mfsmetarestore RUNTIME DESTINATION ${SBIN_SUBDIR})
