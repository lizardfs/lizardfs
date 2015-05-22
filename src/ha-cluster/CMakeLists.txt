configure_file(metadataserver.in metadataserver @ONLY)
configure_file(lizardfs-cluster-manager.in lizardfs-cluster-manager @ONLY)

install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/metadataserver DESTINATION ${OCF_SUBDIR})
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/lizardfs-cluster-manager DESTINATION ${SBIN_SUBDIR})
