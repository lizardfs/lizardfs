include_directories(${CMAKE_CURRENT_SOURCE_DIR})

aux_source_directory(. METADUMP_SOURCES)
add_executable(lfsmetadump ${METADUMP_SOURCES})
target_link_libraries(lfsmetadump)
install(TARGETS lfsmetadump RUNTIME DESTINATION ${SBIN_SUBDIR})
