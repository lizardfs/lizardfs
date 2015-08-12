include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DAPPNAME=mfsmaster)

collect_sources(MASTER)

list(REMOVE_ITEM MASTER_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/chunks.cc")

add_library(master ${MASTER_SOURCES})
target_link_libraries(master mfscommon ${ADDITIONAL_LIBS})
if(JUDY_LIBRARY)
  target_link_libraries(master ${JUDY_LIBRARY})
endif()
add_tests(master ${MASTER_TESTS})

add_executable(mfsmaster ${MAIN_SRC})
target_link_libraries(mfsmaster master)
install(TARGETS mfsmaster RUNTIME DESTINATION ${SBIN_SUBDIR})

configure_file(mfsrestoremaster.in mfsrestoremaster @ONLY)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/mfsrestoremaster DESTINATION ${SBIN_SUBDIR})
