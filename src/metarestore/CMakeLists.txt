include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/src/master)
add_definitions(-DMETARESTORE)
add_definitions(-DAPPNAME=mfsmetarestore)

aux_source_directory(. METARESTORE_SOURCES)
set(METARESTORE_SOURCES ${METARESTORE_SOURCES} ../master/filesystem.cc ../master/chunks.cc)
add_executable(mfsmetarestore ${METARESTORE_SOURCES}) 
target_link_libraries(mfsmetarestore mfscommon)
install(TARGETS mfsmetarestore RUNTIME DESTINATION ${SBIN_SUBDIR})
