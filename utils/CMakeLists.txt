include_directories(${CMAKE_SOURCE_DIR})

# test files generator
add_executable(file-generate file_generate.cc)
install(TARGETS file-generate RUNTIME DESTINATION ${BIN_SUBDIR})

# test files generator which overwrites existing files
add_executable(file-overwrite file_overwrite.cc)
install(TARGETS file-overwrite RUNTIME DESTINATION ${BIN_SUBDIR})

# test files validator
add_executable(file-validate file_validate.cc)
install(TARGETS file-validate RUNTIME DESTINATION ${BIN_SUBDIR})

# a binary for probing different LizardFS processes
add_executable(lizardfs-probe lizardfs_probe.cc)
target_link_libraries(lizardfs-probe mfscommon)
install(TARGETS lizardfs-probe RUNTIME DESTINATION ${BIN_SUBDIR})
