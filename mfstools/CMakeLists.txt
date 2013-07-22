include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../mfscommon)

aux_source_directory(. TOOLS_SOURCES)
add_executable(mfstools ${TOOLS_SOURCES})
target_link_libraries(mfstools mfscommon)
install(TARGETS mfstools DESTINATION bin)
install(CODE "execute_process(COMMAND ./mfstools create WORKING_DIRECTORY ${CMAKE_INSTALL_PREFIX}/bin)")
