aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} MYCRC32_SOURCES)
add_executable(mycrc32 ${MYCRC32_SOURCES})
target_link_libraries(mycrc32 mfscommon)
