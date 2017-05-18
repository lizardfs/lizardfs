#  Copyright 2017 Skytechnology sp. z o.o..
#
#  This file is part of LizardFS.
#
#  LizardFS is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, version 3.
#
#  LizardFS is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with LizardFS  If not, see <http://www.gnu.org/licenses/>.

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
