include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../mfscommon)
set(MAIN_SRC "../mfscommon/main.cc")

add_definitions(-DAPPNAME=mfsmetalogger)  # used for config file paths, locks, messages

add_executable(mfsmetalogger masterconn.cc ${MAIN_SRC}) 
target_link_libraries(mfsmetalogger common ${ZLIB_LIBRARIES})

