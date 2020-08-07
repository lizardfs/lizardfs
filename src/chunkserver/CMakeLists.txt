include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DLIZARDFS_MAX_FILES=10000)
add_definitions(-DAPPNAME=mfschunkserver -DAPP_EXAMPLES_SUBDIR="${CHUNKSERVER_EXAMPLES_SUBDIR}")

collect_sources(CHUNKSERVER)
add_library(chunkserver STATIC ${CHUNKSERVER_SOURCES})
target_link_libraries(chunkserver lzfsprotocol mfscommon ${ADDITIONAL_LIBS})
create_unittest(chunkserver ${CHUNKSERVER_TESTS})
link_unittest(chunkserver chunkserver mfscommon)

add_executable(mfschunkserver ${MAIN_SRC})
target_link_libraries(mfschunkserver chunkserver ${PAM_LIBRARIES})

if(SYSTEMD_FOUND)
  target_link_libraries(mfschunkserver ${SYSTEMD_LIBRARIES})
endif()

install(TARGETS mfschunkserver RUNTIME DESTINATION ${SBIN_SUBDIR})
