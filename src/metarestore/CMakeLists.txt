include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../master)
add_definitions(-DMETARESTORE)
add_definitions(-DAPPNAME=lfsmetarestore)

collect_sources(METARESTORE)

add_library(metarestore ${METARESTORE_SOURCES} ../master/filesystem.cc ../master/chunks.cc ../master/quota_database.cc ../master/chunk_goal_counters.cc)
target_link_libraries(metarestore lfscommon)
target_link_libraries(metarestore)
add_tests(metarestore ${METARESTORE_TESTS})

add_executable(lfsmetarestore ${METARESTORE_MAIN})
target_link_libraries(lfsmetarestore metarestore)
target_link_libraries(lfsmetarestore lfscommon)
target_link_libraries(lfsmetarestore)
install(TARGETS lfsmetarestore RUNTIME DESTINATION ${SBIN_SUBDIR})
