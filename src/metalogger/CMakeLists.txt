include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DMETALOGGER)
add_definitions(-DAPPNAME=mfsmetalogger)

collect_sources(METALOGGER)

add_library(metalogger ${METALOGGER_SOURCES} ../master/changelog.cc ../master/masterconn.cc)
target_link_libraries(metalogger mfscommon)
add_tests(metalogger ${METALOGGER_TESTS})

add_executable(mfsmetalogger ${MAIN_SRC})
target_link_libraries(mfsmetalogger metalogger)
install(TARGETS mfsmetalogger RUNTIME DESTINATION ${SBIN_SUBDIR})
