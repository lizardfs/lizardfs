#  Copyright 2016-2018 Skytechnology sp. z o.o..
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

find_library(FUSE3_LIBRARY fuse3)
find_path(FUSE3_INCLUDE_DIR "fuse3/fuse.h")

if(FUSE3_INCLUDE_DIR)
  set(FUSE3_INCLUDE_DIR "${FUSE3_INCLUDE_DIR}/fuse3")

  file(STRINGS "${FUSE3_INCLUDE_DIR}/fuse_common.h" fuse_version_str REGEX "^#define[\t ]+FUSE.+VERSION[\t ]+[0-9]+")
  string(REGEX REPLACE ".*#define[\t ]+FUSE_MAJOR_VERSION[\t ]+([0-9]+).*" "\\1" fuse_version_major "${fuse_version_str}")
  string(REGEX REPLACE ".*#define[\t ]+FUSE_MINOR_VERSION[\t ]+([0-9]+).*" "\\1" fuse_version_minor "${fuse_version_str}")

  set(FUSE3_VERSION_STRING "${fuse_version_major}.${fuse_version_minor}")
endif()


find_package_handle_standard_args(FUSE3 REQUIRED_VARS FUSE3_LIBRARY FUSE3_INCLUDE_DIR
                                     VERSION_VAR FUSE3_VERSION_STRING)
