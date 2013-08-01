include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../mfscommon)

aux_source_directory(. METADUMP_SOURCES)
add_executable(mfsmetadump ${METADUMP_SOURCES}) 
target_link_libraries(mfsmetadump)
install(TARGETS mfsmetadump RUNTIME DESTINATION ${SBIN_SUBDIR})
