include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../mfscommon)
set(MAIN_SRC "../mfscommon/main.cc")

add_executable(mfsmetalogger masterconn.cc ${MAIN_SRC}) 
target_link_libraries(mfsmetalogger common ${Z_LIB})

