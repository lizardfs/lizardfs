collect_sources(LIZARDFS_PROBE)

add_library(lizardfs-probe-lib ${LIZARDFS_PROBE_SOURCES})
target_link_libraries(lizardfs-probe-lib lfscommon)
add_tests(lizardfs-probe-lib ${LIZARDFS_PROBE_TESTS})

add_executable(lizardfs-probe ${LIZARDFS_PROBE_MAIN})
target_link_libraries(lizardfs-probe lizardfs-probe-lib)
install(TARGETS lizardfs-probe RUNTIME DESTINATION ${BIN_SUBDIR})
