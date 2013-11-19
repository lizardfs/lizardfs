include_directories(${CMAKE_SOURCE_DIR})

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

# test files validator
add_executable(file-validate file_validate.cc)
install(TARGETS file-validate RUNTIME DESTINATION ${BIN_SUBDIR})