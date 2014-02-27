collect_sources(LIZARDFS_PROBE)

add_library(lizardfs-probe-lib ${LIZARDFS_PROBE_SOURCES})
add_tests(lizardfs-probe-lib ${LIZARDFS_PROBE_TESTS})
target_link_libraries(lizardfs-probe-lib mfscommon)

add_executable(lizardfs-probe ${LIZARDFS_PROBE_MAIN})
target_link_libraries(lizardfs-probe lizardfs-probe-lib)
install(TARGETS lizardfs-probe RUNTIME DESTINATION ${BIN_SUBDIR})