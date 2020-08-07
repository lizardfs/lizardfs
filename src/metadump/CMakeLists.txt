include_directories(${CMAKE_CURRENT_SOURCE_DIR})

aux_source_directory(. METADUMP_SOURCES)
add_executable(mfsmetadump ${METADUMP_SOURCES})
install(TARGETS mfsmetadump RUNTIME DESTINATION ${SBIN_SUBDIR})
