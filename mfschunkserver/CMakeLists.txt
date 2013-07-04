include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../mfscommon)
AUX_SOURCE_DIRECTORY("." CHUNKSERVER_SRCS)
set(MAIN_SRC "../mfscommon/main.cc")

add_definitions(-DMFSMAXFILES=10000)
add_definitions(-DAPPNAME=mfschunkserver)  # used for config file paths, locks, messages

add_executable(mfschunkserver ${CHUNKSERVER_SRCS} ${MAIN_SRC})
target_link_libraries(mfschunkserver common ${ZLIB_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
