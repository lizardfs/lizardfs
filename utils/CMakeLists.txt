include_directories(${CMAKE_SOURCE_DIR})

install(PROGRAMS cpp-interpreter.sh DESTINATION ${BIN_SUBDIR})

# a library which causes pread/pwrite/close on chunks to return EIO
add_library(chunk_operations_eio SHARED chunk_operations_eio.c)
if(NOT CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  target_link_libraries(chunk_operations_eio dl)
endif()
install(TARGETS chunk_operations_eio DESTINATION ${LIB_SUBDIR})

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
if(NOT CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  target_link_libraries(redirect_bind dl)
endif()
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

# test files validator
add_executable(file-validate-growing file_validate_growing.cc)
install(TARGETS file-validate-growing RUNTIME DESTINATION ${BIN_SUBDIR})

# utility for flock() system call
add_executable(flockcmd flockcmd.cc)
install(TARGETS flockcmd RUNTIME DESTINATION ${BIN_SUBDIR})

# utility for posix locks
add_executable(posixlockcmd posixlockcmd.cc)
install(TARGETS posixlockcmd RUNTIME DESTINATION ${BIN_SUBDIR})

# ping client
add_executable(mfsping mfs_ping.cc)
target_link_libraries(mfsping mfscommon)
install(TARGETS mfsping RUNTIME DESTINATION ${BIN_SUBDIR})

# ping server
add_executable(mfspingserv mfs_pingserv.cc)
target_link_libraries(mfspingserv mfscommon)
install(TARGETS mfspingserv RUNTIME DESTINATION ${BIN_SUBDIR})

# ping pong fcntl lock test
add_executable(lzfs_ping_pong ping_pong.cc)
install(TARGETS lzfs_ping_pong RUNTIME DESTINATION ${BIN_SUBDIR})

add_library(slow_chunk_scan SHARED slow_chunk_scan.c)
if(NOT CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  target_link_libraries(slow_chunk_scan dl)
endif()
install(TARGETS slow_chunk_scan DESTINATION ${LIB_SUBDIR})
