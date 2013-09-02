cmake_minimum_required(VERSION 2.8)
project(crcutil)

set(CMAKE_CXX_FLAGS "-msse4.2")
include_directories( ${CMAKE_CURRENT_SOURCE_DIR}/code
    ${CMAKE_CURRENT_SOURCE_DIR}/examples
    ${CMAKE_CURRENT_SOURCE_DIR}/tests )

aux_source_directory(./code COMMON_SOURCES )
add_library(crcutil ${COMMON_SOURCES} ./examples/interface.cc )


