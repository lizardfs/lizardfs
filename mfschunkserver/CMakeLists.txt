include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../mfscommon)
add_definitions(-DMFSMAXFILES=10000)
add_definitions(-DAPPNAME=mfschunkserver)

aux_source_directory(. CHUNKSERVER_SOURCES)
add_executable(mfschunkserver ${CHUNKSERVER_SOURCES} ${MAIN_SRC})
target_link_libraries(mfschunkserver mfscommon ${ZLIB_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
install(TARGETS mfschunkserver RUNTIME DESTINATION sbin)
