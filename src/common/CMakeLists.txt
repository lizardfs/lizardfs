include_directories(${CMAKE_CURRENT_SOURCE_DIR})

aux_source_directory(. COMMON_SOURCES)
add_library(mfscommon ${COMMON_SOURCES})
