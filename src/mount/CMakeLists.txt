include_directories(${CMAKE_CURRENT_SOURCE_DIR})

collect_sources(MOUNT)

add_library(mount ${MOUNT_SOURCES})
target_link_libraries(mount mfscommon ${ADDITIONAL_LIBS} ${Boost_THREAD_LIBRARY})

create_unittest(mount ${MOUNT_TESTS})
link_unittest(mount mount mfscommon)

install(PROGRAMS mfssnapshot DESTINATION ${BIN_SUBDIR})

if (ENABLE_CLIENT_LIB)
  add_library(mount_pic ${MOUNT_SOURCES})
  if("${CMAKE_VERSION}" VERSION_GREATER 2.8.9)
    set_property(TARGET mount_pic PROPERTY POSITION_INDEPENDENT_CODE ON)
  else()
    set_property(TARGET mount_pic PROPERTY COMPILE_FLAGS "-fPIC")
  endif()
  target_link_libraries(mount_pic mfscommon_pic ${ADDITIONAL_LIBS})

  add_subdirectory(client)
endif()
