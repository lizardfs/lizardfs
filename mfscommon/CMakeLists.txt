include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../3rdparty/crcutil/examples ../3rdparty/crcutil/code )
aux_source_directory(. COMMON_SOURCES)
add_library(mfscommon ${COMMON_SOURCES})
target_link_libraries(mfscommon crcutil)
