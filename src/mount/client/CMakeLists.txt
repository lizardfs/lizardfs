collect_sources(CLIENT)

add_library(lizardfs-client-static client.cc lizardfs_c_api.cc client_error_code.cc)
set_target_properties(lizardfs-client-static PROPERTIES OUTPUT_NAME "lizardfs-client")
add_library(lizardfs-client SHARED client.cc lizardfs_c_api.cc client_error_code.cc)
add_library(lizardfs-client-cpp client.cc client_error_code.cc)

target_link_libraries(lizardfs-client-static mount_pic)
target_link_libraries(lizardfs-client mount_pic)
target_link_libraries(lizardfs-client-cpp mount_pic)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  target_link_libraries(lizardfs-client-static dl)
  target_link_libraries(lizardfs-client dl)
  target_link_libraries(lizardfs-client-cpp dl)
endif()

add_library(lizardfsmount_shared SHARED ${MOUNT_SOURCES} lizard_client_c_linkage.cc)
target_link_libraries(lizardfsmount_shared mount_pic ${ADDITIONAL_LIBS})
install(TARGETS lizardfsmount_shared DESTINATION ${LIB_SUBDIR})

install(TARGETS lizardfs-client-static DESTINATION ${LIB_SUBDIR})
install(TARGETS lizardfs-client-cpp DESTINATION ${LIB_SUBDIR})
install(TARGETS lizardfs-client DESTINATION ${LIB_SUBDIR})

install(FILES lizardfs_c_api.h DESTINATION ${INCL_SUBDIR})
install(FILES ../../common/lizardfs_error_codes.h DESTINATION ${INCL_SUBDIR})
