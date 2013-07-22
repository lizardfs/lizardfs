include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../mfscommon)
add_definitions(-DAPPNAME=mfsmetalogger)

aux_source_directory(. METALOGGER_SOURCES)
add_executable(mfsmetalogger ${METALOGGER_SOURCES} ${MAIN_SRC}) 
target_link_libraries(mfsmetalogger mfscommon ${ZLIB_LIBRARIES})
install(TARGETS mfsmetalogger RUNTIME DESTINATION sbin)
