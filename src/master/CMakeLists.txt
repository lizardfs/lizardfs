include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DAPPNAME=mfsmaster)

collect_sources(MASTER)

add_library(master ${MASTER_SOURCES})
target_link_libraries(master mfscommon ${ZLIB_LIBRARIES}
    ${ADDITIONAL_LIBS}
    ${Boost_FILESYSTEM_LIBRARY} ${Boost_SYSTEM_LIBRARY})
add_tests(master ${MASTER_TESTS})

set(RESTORE_SRC ${CMAKE_SOURCE_DIR}/src/metarestore/restore.cc)
add_executable(mfsmaster ${MAIN_SRC} ${RESTORE_SRC})
target_link_libraries(mfsmaster master)
install(TARGETS mfsmaster RUNTIME DESTINATION ${SBIN_SUBDIR})

configure_file(mfsrestoremaster.in mfsrestoremaster @ONLY)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/mfsrestoremaster DESTINATION ${SBIN_SUBDIR})
