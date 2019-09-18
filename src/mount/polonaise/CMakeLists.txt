if(NOT Boost_PROGRAM_OPTIONS_FOUND)
  message(STATUS "Boost.Program_options library not found - building lizardfs-polonaise-server skipped")
  return()
endif()
if(NOT POLONAISE_FOUND)
  message(STATUS "Polonaise library not found - building lizardfs-polonaise-server skipped")
  return()
endif()
if(NOT THRIFT_FOUND)
  message(STATUS "Thrift library not found - building lizardfs-polonaise-server skipped")
  return()
endif()

collect_sources(MOUNT_POLONAISE)

# Make binaries in the build tree and in install tree use proper rpaths to shared libraries
set(CMAKE_BUILD_WITH_INSTALL_RPATH true)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH true)

include_directories(${Boost_INCLUDE_DIRS} ${POLONAISE_INCLUDE_DIRS} ${THRIFT_INCLUDE_DIRS})
add_executable(lizardfs-polonaise-server ${MOUNT_POLONAISE_MAIN} ${MOUNT_POLONAISE_SOURCES})
target_link_libraries(lizardfs-polonaise-server mfscommon mount ${POLONAISE_LIBRARIES}
${THRIFT_LIBRARIES} ${Boost_PROGRAM_OPTIONS_LIBRARY} ${Boost_SYSTEM_LIBRARY})

install(TARGETS lizardfs-polonaise-server RUNTIME DESTINATION ${BIN_SUBDIR})
