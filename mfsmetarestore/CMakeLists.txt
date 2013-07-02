include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../mfscommon ../mfsmaster)
AUX_SOURCE_DIRECTORY("." METARESTORE_SRCS)
set(FILESYSTEM_SRC ../mfsmaster/filesystem.cc ../mfsmaster/chunks.cc)
set(STDERR_SRC ../mfscommon/strerr.cc)

add_definitions(-DMETARESTORE)

add_executable(mfsmetarestore ${METARESTORE_SRCS} ${FILESYSTEM_SRC} ${STDERR_SRC}) 
target_link_libraries(mfsmetarestore ${Z_LIB})

