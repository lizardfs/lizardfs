collect_sources(CLIENT)

shared_add_library(lizardfs-client client.cc lizardfs_c_api.cc client_error_code.cc
                   crash-log.cc)
shared_target_link_libraries(lizardfs-client mount)

shared_add_library(lizardfs-client-cpp client.cc client_error_code.cc
                   crash-log.cc)
shared_target_link_libraries(lizardfs-client-cpp mount)

shared_target_link_libraries(lizardfs-client ${CMAKE_DL_LIBS})

add_library(lizardfs-client_shared SHARED client.cc lizardfs_c_api.cc client_error_code.cc
            crash-log.cc)
set_target_properties(lizardfs-client_shared PROPERTIES OUTPUT_NAME "lizardfs-client")
target_link_libraries(lizardfs-client_shared ${CMAKE_DL_LIBS} mount_pic)

add_library(lizardfsmount_shared SHARED ${MOUNT_SOURCES} lizard_client_c_linkage.cc
            crash-log.cc)
target_link_libraries(lizardfsmount_shared mount_pic ${ADDITIONAL_LIBS})

install(TARGETS lizardfs-client DESTINATION ${LIB_SUBDIR})
install(TARGETS lizardfs-client_pic DESTINATION ${LIB_SUBDIR})
install(TARGETS lizardfs-client-cpp DESTINATION ${LIB_SUBDIR})
install(TARGETS lizardfs-client-cpp_pic DESTINATION ${LIB_SUBDIR})
install(TARGETS lizardfsmount_shared DESTINATION ${LIB_SUBDIR})
install(TARGETS lizardfs-client_shared DESTINATION ${LIB_SUBDIR})

install(FILES lizardfs_c_api.h DESTINATION ${INCL_SUBDIR})
install(FILES ../../common/lizardfs_error_codes.h DESTINATION ${INCL_SUBDIR})
