include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DMETALOGGER)
add_definitions(-DAPPNAME=lfsmetalogger)

collect_sources(METALOGGER)

add_library(metalogger ${METALOGGER_SOURCES})
target_link_libraries(metalogger lfscommon)
target_link_libraries(metalogger)
add_tests(metalogger ${METALOGGER_TESTS})

add_executable(lfsmetalogger ${MAIN_SRC})
target_link_libraries(lfsmetalogger metalogger)
install(TARGETS lfsmetalogger RUNTIME DESTINATION ${SBIN_SUBDIR})
