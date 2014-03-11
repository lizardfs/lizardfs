include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../master)
add_definitions(-DMETARESTORE)
add_definitions(-DAPPNAME=mfsmetarestore)

collect_sources(METARESTORE)

add_library(metarestore ${METARESTORE_SOURCES} ../master/filesystem.cc ../master/chunks.cc ../master/metadata_dumper.cc)
target_link_libraries(metarestore mfscommon)
add_tests(metarestore ${METARESTORE_TESTS})

add_executable(mfsmetarestore ${METARESTORE_MAIN})
target_link_libraries(mfsmetarestore metarestore)
install(TARGETS mfsmetarestore RUNTIME DESTINATION ${SBIN_SUBDIR})
