include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../mfscommon)

add_executable(mfsmetadump mfsmetadump.cc) 
target_link_libraries(mfsmetadump)

