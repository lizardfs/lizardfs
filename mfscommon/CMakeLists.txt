aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} MFSCOMMON_SOURCES)
add_library(mfscommon ${MFSCOMMON_SOURCES})
target_link_libraries(mfscommon ${MFSCOMMON_ADDITIONAL_LIBS})
