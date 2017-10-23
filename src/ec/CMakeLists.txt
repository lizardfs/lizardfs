include_directories(${CMAKE_CURRENT_SOURCE_DIR})

collect_sources(EC)

add_executable(ec ${EC_MAIN} ${EC_SOURCES})
target_link_libraries(ec mfscommon)
install(TARGETS ec DESTINATION ${BIN_SUBDIR})
