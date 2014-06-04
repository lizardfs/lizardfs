include_directories(${CMAKE_SOURCE_DIR})

install(PROGRAMS cpp-interpreter.sh DESTINATION ${BIN_SUBDIR})

# converter to xor chunks
add_executable(chunk_converter chunk_converter.cc)
target_link_libraries(chunk_converter ${ZLIB_LIBRARY})
install(TARGETS chunk_converter RUNTIME DESTINATION ${BIN_SUBDIR})

# crc converter
add_executable(crc_converter crc_converter.cc)
target_link_libraries(crc_converter ${ZLIB_LIBRARY})
install(TARGETS crc_converter RUNTIME DESTINATION ${BIN_SUBDIR})

# redirecting bind library
add_library(redirect_bind SHARED redirect_bind.c)
target_link_libraries(redirect_bind dl)
install(TARGETS redirect_bind DESTINATION ${LIB_SUBDIR})

# test files generator
add_executable(file-generate file_generate.cc)
install(TARGETS file-generate RUNTIME DESTINATION ${BIN_SUBDIR})

# test files generator which overwrites existing files
add_executable(file-overwrite file_overwrite.cc)
install(TARGETS file-overwrite RUNTIME DESTINATION ${BIN_SUBDIR})

# test files validator
add_executable(file-validate file_validate.cc)
install(TARGETS file-validate RUNTIME DESTINATION ${BIN_SUBDIR})

# ping client
add_executable(mfsping mfs_ping.cc)
target_link_libraries(mfsping mfscommon)
install(TARGETS mfsping RUNTIME DESTINATION ${BIN_SUBDIR})

# ping server
add_executable(mfspingserv mfs_pingserv.cc)
target_link_libraries(mfspingserv mfscommon)
install(TARGETS mfspingserv RUNTIME DESTINATION ${BIN_SUBDIR})
