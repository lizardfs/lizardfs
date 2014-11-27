include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DLFSMAXFILES=10000)
add_definitions(-DAPPNAME=lfschunkserver)

collect_sources(CHUNKSERVER)
add_library(chunkserver ${CHUNKSERVER_SOURCES})
target_link_libraries(chunkserver lfscommon)
add_tests(chunkserver ${CHUNKSERVER_TESTS})

add_executable(lfschunkserver ${MAIN_SRC})
target_link_libraries(lfschunkserver chunkserver)
install(TARGETS lfschunkserver RUNTIME DESTINATION ${SBIN_SUBDIR})
