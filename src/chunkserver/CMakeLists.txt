include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DMFSMAXFILES=10000)
add_definitions(-DAPPNAME=mfschunkserver)

collect_sources(CHUNKSERVER)
add_library(chunkserver ${CHUNKSERVER_SOURCES})
target_link_libraries(chunkserver mfscommon ${CMAKE_THREAD_LIBS_INIT} ${ADDITIONAL_LIBS})
add_tests(chunkserver ${CHUNKSERVER_TESTS})

add_executable(mfschunkserver ${MAIN_SRC})
target_link_libraries(mfschunkserver chunkserver)
install(TARGETS mfschunkserver RUNTIME DESTINATION ${SBIN_SUBDIR})
