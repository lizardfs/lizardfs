collect_sources(MOUNT_POLONAISE)

# Make binaries in the build tree and in install tree use proper rpaths to shared libraries
set(CMAKE_BUILD_WITH_INSTALL_RPATH true)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH true)

add_executable(lizardfs-polonaise-server ${MOUNT_POLONAISE_MAIN} ${MOUNT_POLONAISE_SOURCES})
target_link_libraries(lizardfs-polonaise-server mfscommon mount ${POLONAISE_LIBRARIES} ${THRIFT_LIBRARIES} ${Boost_PROGRAM_OPTIONS_LIBRARY})
include_directories(${POLONAISE_INCLUDE_DIRS} ${THRIFT_INCLUDE_DIRS})
install(TARGETS lizardfs-polonaise-server RUNTIME DESTINATION ${BIN_SUBDIR})

