include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DAPPNAME=lfsmaster)

collect_sources(MASTER)

add_library(master ${MASTER_SOURCES})
target_link_libraries(master lfscommon)
add_tests(master ${MASTER_TESTS})

set(RESTORE_SRC ${CMAKE_SOURCE_DIR}/src/metarestore/restore.cc)
set(METALOGGER_SRC ${CMAKE_SOURCE_DIR}/src/metalogger/masterconn.cc)
add_executable(lfsmaster ${MAIN_SRC} ${RESTORE_SRC} ${METALOGGER_SRC})
target_link_libraries(lfsmaster master)
install(TARGETS lfsmaster RUNTIME DESTINATION ${SBIN_SUBDIR})

configure_file(lfsrestoremaster.in lfsrestoremaster @ONLY)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/lfsrestoremaster DESTINATION ${SBIN_SUBDIR})
