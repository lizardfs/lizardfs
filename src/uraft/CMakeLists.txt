configure_file(lizardfs-uraft-helper.in lizardfs-uraft-helper @ONLY)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

collect_sources(URAFT)
set(URAFT_SOURCES ${URAFT_SOURCES} ${CMAKE_SOURCE_DIR}/src/common/time_utils.cc)

add_executable(lizardfs-uraft ${URAFT_MAIN} ${URAFT_SOURCES})

target_link_libraries(lizardfs-uraft ${Boost_LIBRARIES} ${RT_LIBRARY} pthread)

install(TARGETS lizardfs-uraft RUNTIME DESTINATION ${SBIN_SUBDIR})
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/lizardfs-uraft-helper DESTINATION ${SBIN_SUBDIR})
