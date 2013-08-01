include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../mfscommon)
add_definitions(-DAPPNAME=mfsmaster)

aux_source_directory(. MASTER_SOURCES)
add_executable(mfsmaster ${MASTER_SOURCES} ${MAIN_SRC}) 
target_link_libraries(mfsmaster mfscommon ${ZLIB_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
install(TARGETS mfsmaster RUNTIME DESTINATION ${SBIN_SUBDIR})
