include(CheckCXXSourceRuns)

function(check_cxx_expression _EXPR _HEADER _RESULT)
  set(_INCLUDE_FILES)
  foreach (it ${_HEADER})
    set(_INCLUDE_FILES "${_INCLUDE_FILES}#include <${it}>\n")
  endforeach ()

  set(_CHECK_CXX_EXPRESSION_SOURCE_CODE "
${_INCLUDE_FILES}

template<bool value>
struct bool_value {
};

template<>
struct bool_value<true> {
   typedef int value_type;
};

int main() {
    typename bool_value<(bool)(${_EXPR})>::value_type r = 0;
    return r;
}
")

  CHECK_CXX_SOURCE_COMPILES("${_CHECK_CXX_EXPRESSION_SOURCE_CODE}" ${_RESULT})
endfunction()
