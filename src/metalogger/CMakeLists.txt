include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DAPPNAME=mfsmetalogger)

collect_sources(METALOGGER)

add_library(metalogger ${METALOGGER_SOURCES})
target_link_libraries(metalogger mfscommon ${ZLIB_LIBRARIES})
target_link_libraries(metalogger ${Boost_FILESYSTEM_LIBRARY} ${Boost_SYSTEM_LIBRARY})
add_tests(metalogger ${METALOGGER_TESTS})

add_executable(mfsmetalogger ${MAIN_SRC})
target_link_libraries(mfsmetalogger metalogger)
install(TARGETS mfsmetalogger RUNTIME DESTINATION ${SBIN_SUBDIR})
