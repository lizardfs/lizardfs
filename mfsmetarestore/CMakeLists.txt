include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../mfscommon ../mfsmaster)
add_definitions(-DMETARESTORE)
add_definitions(-DAPPNAME=mfsmetarestore)

aux_source_directory(. METARESTORE_SOURCES)
set(METARESTORE_SOURCES ${METARESTORE_SOURCES} ../mfsmaster/filesystem.cc ../mfsmaster/chunks.cc)
add_executable(mfsmetarestore ${METARESTORE_SOURCES}) 
target_link_libraries(mfsmetarestore mfscommon)
install(TARGETS mfsmetarestore RUNTIME DESTINATION sbin)
