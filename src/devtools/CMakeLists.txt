aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} DEVTOOLS_SOURCES)
add_library(devtools STATIC ${DEVTOOLS_SOURCES})

add_subdirectory(mycrc32)
