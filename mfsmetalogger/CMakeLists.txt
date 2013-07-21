add_definitions(-DAPPNAME=mfsmetalogger)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}) # Needed by include "init.h" in main.cc

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} MFSMETALOGGER_SOURCES)
add_executable(mfsmetalogger ${MFSMETALOGGER_SOURCES} ${MAIN_CC})
target_link_libraries(mfsmetalogger mfscommon ${M_LIB} ${Z_LIB})
install(TARGETS mfsmetalogger RUNTIME DESTINATION sbin)
