include_directories(${CMAKE_CURRENT_SOURCE_DIR})

collect_sources(MOUNT)

shared_add_library(mount ${MOUNT_SOURCES})
shared_target_link_libraries(mount mfscommon ${ADDITIONAL_LIBS})

create_unittest(mount ${MOUNT_TESTS})
link_unittest(mount mount mfscommon)

if (ENABLE_CLIENT_LIB)
  add_subdirectory(client)
endif()
