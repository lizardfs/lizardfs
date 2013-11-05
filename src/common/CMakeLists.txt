include_directories(${CMAKE_CURRENT_SOURCE_DIR})

aux_source_directory(. COMMON_SOURCES)
add_library(mfscommon ${COMMON_SOURCES})
target_link_libraries(mfscommon ${MFSCOMMON_ADDITIONAL_LIBS})
if (UNIX AND NOT APPLE)
  target_link_libraries(mfscommon ${RT_LIB})
endif()
