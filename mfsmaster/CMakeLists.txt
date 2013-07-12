add_definitions(-DAPPNAME=mfsmaster)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}) # Needed by include "init.h" in main.cc

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} MFSMASTER_SOURCES)
add_executable(mfsmaster ${MFSMASTER_SOURCES} ${MAIN_CC})
target_link_libraries(mfsmaster mfscommon ${M_LIB} ${Z_LIB})
install(TARGETS mfsmaster RUNTIME DESTINATION sbin)
