#  Copyright 2016-2017 Skytechnology sp. z o.o..
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

# - Try to find the Judy libraries
#
#  JUDY_FOUND - system has Judy
#  JUDY_INCLUDE_DIR - the Judy include directory

include(CheckCSourceRuns)

find_path(JUDY_INCLUDE_DIR NAMES Judy.h)
find_library(JUDY_LIBRARY Judy)

set(CMAKE_REQUIRED_INCLUDES ${JUDY_INCLUDE_DIR})
set(CMAKE_REQUIRED_LIBRARIES ${JUDY_LIBRARY})
set(_CHECK_FOR_JUDY1_BUG "
#include <Judy.h>
int main() {
  Pvoid_t judy_array = NULL;
  int i, value;
  JError_t error;
  for (i=1;i<50000000;i++) {
    value = Judy1Set(&judy_array, i, &error);
    if (value == JERR || !value) return 1;
  }
  return 0;
}
")
check_c_source_runs("${_CHECK_FOR_JUDY1_BUG}" JUDY_HAVE_WORKING_JUDY1)
unset(CMAKE_REQUIRED_INCLUDES)
unset(CMAKE_REQUIRED_LIBRARIES)

find_package_handle_standard_args(JUDY
                                  REQUIRED_VARS JUDY_LIBRARY JUDY_INCLUDE_DIR)
