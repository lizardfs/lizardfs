include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DAPPNAME=mfsmaster -DAPP_EXAMPLES_SUBDIR="${MFSMASTER_EXAMPLES_SUBDIR}")

collect_sources(MASTER)

if(NOT DB_FOUND)
  list(REMOVE_ITEM MASTER_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/hstring_bdbstorage.cc")
endif()

add_library(master STATIC ${MASTER_SOURCES})
target_link_libraries(master mfscommon ${ADDITIONAL_LIBS})
if(JUDY_LIBRARY)
  target_link_libraries(master ${JUDY_LIBRARY})
endif()
if(DB_FOUND)
  target_link_libraries(master ${DB_LIBRARY})
endif()
create_unittest(master ${MASTER_TESTS})
link_unittest(master master mfscommon)

add_executable(mfsmaster ${MAIN_SRC})
target_link_libraries(mfsmaster master ${PAM_LIBRARIES})
if(SYSTEMD_FOUND)
  target_link_libraries(mfsmaster ${SYSTEMD_LIBRARIES})
endif()
install(TARGETS mfsmaster RUNTIME DESTINATION ${SBIN_SUBDIR})

configure_file(mfsrestoremaster.in mfsrestoremaster @ONLY)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/mfsrestoremaster DESTINATION ${SBIN_SUBDIR})
