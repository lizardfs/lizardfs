include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../master)
add_definitions(-DMETARESTORE)
add_definitions(-DAPPNAME=mfsmetarestore)

collect_sources(METARESTORE)

add_library(metarestore ${METARESTORE_SOURCES} ../master/filesystem.cc ../master/chunks.cc ../master/quota_database.cc ../master/chunk_goal_counters.cc)
target_link_libraries(metarestore mfscommon)
target_link_libraries(metarestore)
add_tests(metarestore ${METARESTORE_TESTS})

add_executable(mfsmetarestore ${METARESTORE_MAIN})
target_link_libraries(mfsmetarestore metarestore)
target_link_libraries(mfsmetarestore mfscommon)
target_link_libraries(mfsmetarestore)
install(TARGETS mfsmetarestore RUNTIME DESTINATION ${SBIN_SUBDIR})
