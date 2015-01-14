include_directories(${CMAKE_SOURCE_DIR})

install(PROGRAMS cpp-interpreter.sh DESTINATION ${BIN_SUBDIR})

# a library which causes pread/pwrite/close on chunks to return EIO
add_library(chunk_operations_eio SHARED chunk_operations_eio.c)
target_link_libraries(chunk_operations_eio dl)
install(TARGETS chunk_operations_eio DESTINATION ${LIB_SUBDIR})

# test files generator
add_executable(file-generate file_generate.cc)
install(TARGETS file-generate RUNTIME DESTINATION ${BIN_SUBDIR})

# test files generator which overwrites existing files
add_executable(file-overwrite file_overwrite.cc)
install(TARGETS file-overwrite RUNTIME DESTINATION ${BIN_SUBDIR})

# test files validator
add_executable(file-validate file_validate.cc)
install(TARGETS file-validate RUNTIME DESTINATION ${BIN_SUBDIR})
