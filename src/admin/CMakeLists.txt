collect_sources(LIZARDFS_ADMIN)

add_library(lizardfs-admin-lib STATIC ${LIZARDFS_ADMIN_SOURCES})
target_link_libraries(lizardfs-admin-lib mfscommon)
create_unittest(lizardfs-admin-lib ${LIZARDFS_ADMIN_TESTS})
link_unittest(lizardfs-admin-lib mfscommon)

add_executable(lizardfs-admin ${LIZARDFS_ADMIN_MAIN})
target_link_libraries(lizardfs-admin lizardfs-admin-lib)
install(TARGETS lizardfs-admin RUNTIME DESTINATION ${BIN_SUBDIR})

add_custom_target(lizardfs-probe ALL
    COMMAND ln -sf lizardfs-admin lizardfs-probe
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    DEPENDS lizardfs-admin)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/lizardfs-probe DESTINATION ${BIN_SUBDIR})
