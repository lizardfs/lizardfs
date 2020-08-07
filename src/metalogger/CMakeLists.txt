include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DMETALOGGER)
add_definitions(-DAPPNAME=mfsmetalogger -DAPP_EXAMPLES_SUBDIR="${METALOGGER_EXAMPLES_SUBDIR}")

collect_sources(METALOGGER)

add_library(metalogger STATIC ${METALOGGER_SOURCES} ../master/changelog.cc ../master/masterconn.cc)
target_link_libraries(metalogger mfscommon)

create_unittest(metalogger ${METALOGGER_TESTS})

add_executable(mfsmetalogger ${MAIN_SRC})
target_link_libraries(mfsmetalogger metalogger ${PAM_LIBRARIES})
if(SYSTEMD_FOUND)
  target_link_libraries(mfsmetalogger ${SYSTEMD_LIBRARIES})
endif()
install(TARGETS mfsmetalogger RUNTIME DESTINATION ${SBIN_SUBDIR})
