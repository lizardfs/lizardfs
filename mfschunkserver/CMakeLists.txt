add_definitions(-DAPPNAME=mfschunkserver)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}) # Needed by include "init.h" in main.cc

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} CHUNKSERVER_SOURCES)
add_library(chunkserver ${CHUNKSERVER_SOURCES})
target_link_libraries(chunkserver mfscommon ${M_LIB} ${Z_LIB})

add_executable(mfschunkserver ${MAIN_CC})
target_link_libraries(mfschunkserver chunkserver)
install(TARGETS mfschunkserver RUNTIME DESTINATION sbin)
