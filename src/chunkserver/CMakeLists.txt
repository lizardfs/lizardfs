include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DMFSMAXFILES=10000)
add_definitions(-DAPPNAME=mfschunkserver)

aux_source_directory(. CHUNKSERVER_SOURCES)
add_library(chunkserver STATIC ${CHUNKSERVER_SOURCES})
target_link_libraries(chunkserver mfscommon)

add_executable(mfschunkserver ${MAIN_SRC})
target_link_libraries(mfschunkserver mfscommon chunkserver ${ZLIB_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
install(TARGETS mfschunkserver RUNTIME DESTINATION ${SBIN_SUBDIR})
