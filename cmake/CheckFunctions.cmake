function(check_functions FUNCTIONS REQUIRED)
  foreach(FUNC ${FUNCTIONS})
    string(TOUPPER "LIZARDFS_HAVE_${FUNC}" VAR)
    CHECK_FUNCTION_EXISTS(${FUNC} ${VAR})
    if(${REQUIRED})
      if("${${VAR}}" STREQUAL "" OR NOT ${${VAR}} EQUAL 1)
        message(SEND_ERROR "function ${FUNC} is required")
      endif()
    endif()
  endforeach()
endfunction()

function(check_template_function_exists HEADER CALL OUTPUT_VARIABLE)
	if (NOT DEFINED ${OUTPUT_VARIABLE})
		check_cxx_source_compiles("
#include <${HEADER}>
	int main(int, char**) {
		${CALL};
		return 0;
}" ${OUTPUT_VARIABLE})
	endif()
endfunction()
