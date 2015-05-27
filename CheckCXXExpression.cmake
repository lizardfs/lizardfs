include(CheckCXXSourceRuns)

function(check_cxx_expression _EXPR _HEADER _RESULT)
  set(_INCLUDE_FILES)
  foreach (it ${_HEADER})
    set(_INCLUDE_FILES "${_INCLUDE_FILES}#include <${it}>\n")
  endforeach ()

  set(_CHECK_CXX_EXPRESSION_SOURCE_CODE "
${_INCLUDE_FILES}
int main()
{
    if( ${_EXPR} ) return 0;
    return 1;
}
")

  CHECK_CXX_SOURCE_RUNS("${_CHECK_CXX_EXPRESSION_SOURCE_CODE}" ${_RESULT})
endfunction()
