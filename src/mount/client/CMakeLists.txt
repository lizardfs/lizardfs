collect_sources(CLIENT)

add_library(lizardfs-client SHARED ${CLIENT_SOURCES})

target_link_libraries(lizardfs-client mount_pic)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  target_link_libraries(lizardfs-client dl)
endif()

add_library(lizardfsmount_shared SHARED ${MOUNT_SOURCES} lizard_client_c_linkage.cc)
target_link_libraries(lizardfsmount_shared mount_pic ${ADDITIONAL_LIBS})
install(TARGETS lizardfsmount_shared DESTINATION ${LIB_SUBDIR})

install(TARGETS lizardfs-client DESTINATION ${LIB_SUBDIR})
