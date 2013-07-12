aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} MFSTOOLS_SOURCES)
add_executable(mfstools ${MFSTOOLS_SOURCES})
target_link_libraries(mfstools mfscommon ${M_LIB})
install(TARGETS mfstools RUNTIME DESTINATION bin)
