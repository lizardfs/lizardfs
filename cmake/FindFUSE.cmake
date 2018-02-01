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

if(APPLE)
  find_package(PkgConfig REQUIRED)
  if(PKG_CONFIG_FOUND)
    pkg_check_modules(PC_FUSE fuse)
  endif(PKG_CONFIG_FOUND)

  if(PC_FUSE_FOUND)
    set(FUSE_LIBRARY ${PC_FUSE_LIBRARIES})
    set(FUSE_LIBRARY_DIR ${PC_FUSE_LIBRARY_DIRS})
    set(FUSE_CFLAGS ${PC_FUSE_CFLAGS})
    set(FUSE_CFLAGS_OTHER  ${PC_FUSE_CFLAGS_OTHER})
    set(FUSE_VERSION_STRING ${PC_FUSE_VERSION})
    find_path(FUSE_INCLUDE_DIR "fuse.h" PATHS ${PC_FUSE_INCLUDE_DIRS} ${PC_FUSE_INCLUDE_DIRS}/.. PATH_SUFFIXES "fuse")
  endif()
else()
  find_library(FUSE_LIBRARY fuse)
  find_path(FUSE_INCLUDE_DIR "fuse/fuse.h")

  if(FUSE_INCLUDE_DIR)
    set(FUSE_INCLUDE_DIR "${FUSE_INCLUDE_DIR}/fuse")

    file(STRINGS "${FUSE_INCLUDE_DIR}/fuse_common.h" fuse_version_str REGEX "^#define[\t ]+FUSE.+VERSION[\t ]+[0-9]+")
    string(REGEX REPLACE ".*#define[\t ]+FUSE_MAJOR_VERSION[\t ]+([0-9]+).*" "\\1" fuse_version_major "${fuse_version_str}")
    string(REGEX REPLACE ".*#define[\t ]+FUSE_MINOR_VERSION[\t ]+([0-9]+).*" "\\1" fuse_version_minor "${fuse_version_str}")

    set(FUSE_VERSION_STRING "${fuse_version_major}.${fuse_version_minor}")
  endif()
endif()

find_package_handle_standard_args(FUSE REQUIRED_VARS FUSE_LIBRARY FUSE_INCLUDE_DIR
                                     VERSION_VAR FUSE_VERSION_STRING)
