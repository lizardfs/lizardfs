# Set SYSTEM_ARCHITECTURE with current system architecture #
############################################################

# dpkg
execute_process(COMMAND dpkg --print-architecture TIMEOUT 1 RESULT_VARIABLE CODE_OF_RETURN OUTPUT_VARIABLE SYSTEM_ARCHITECTURE)
if(NOT CODE_OF_RETURN STREQUAL "0")
# arch
  execute_process(COMMAND arch TIMEOUT 1 RESULT_VARIABLE CODE_OF_RETURN OUTPUT_VARIABLE SYSTEM_ARCHITECTURE)
endif()
if(NOT CODE_OF_RETURN STREQUAL "0")
# uname
  execute_process(COMMAND uname -m TIMEOUT 1 RESULT_VARIABLE CODE_OF_RETURN OUTPUT_VARIABLE SYSTEM_ARCHITECTURE)
endif()
if(NOT CODE_OF_RETURN STREQUAL "0")
# none
  message(WARNING "Can't obtain system architecture, using 'i386'")
  set(SYSTEM_ARCHITECTURE "i386")
else()
# parsing
  string(LENGTH ${SYSTEM_ARCHITECTURE} SA_LENGTH)
  math(EXPR SA_LENGTH "${SA_LENGTH} -1")
  string(SUBSTRING ${SYSTEM_ARCHITECTURE} 0 ${SA_LENGTH} SYSTEM_ARCHITECTURE)
endif()
message(STATUS "SYSTEM_ARCHITECTURE='${SYSTEM_ARCHITECTURE}'")
