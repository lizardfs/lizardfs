include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DAPPNAME=mfsmaster)

collect_sources(MASTER)

add_library(master ${MASTER_SOURCES})
target_link_libraries(master mfscommon ${ZLIB_LIBRARIES})
add_tests(master ${MASTER_TESTS})

add_executable(mfsmaster ${MAIN_SRC})
target_link_libraries(mfsmaster master)
install(TARGETS mfsmaster RUNTIME DESTINATION ${SBIN_SUBDIR})
