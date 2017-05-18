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

find_library(DB_LIBRARY db)
find_path(DB_INCLUDE_DIR db.h)

if(DB_INCLUDE_DIR)
  file(STRINGS "${DB_INCLUDE_DIR}/db.h" db_version_str REGEX "^#define[\t ]+DB_VERSION[^\t ]+[\t ]+[0-9]+")
  string(REGEX REPLACE ".*#define[\t ]+DB_VERSION_FAMILY[\t ]+([0-9]+).*" "\\1" db_version_family "${db_version_str}")
  string(REGEX REPLACE ".*#define[\t ]+DB_VERSION_RELEASE[\t ]+([0-9]+).*" "\\1" db_version_release "${db_version_str}")
  string(REGEX REPLACE ".*#define[\t ]+DB_VERSION_MAJOR[\t ]+([0-9]+).*" "\\1" db_version_major "${db_version_str}")
  string(REGEX REPLACE ".*#define[\t ]+DB_VERSION_MINOR[\t ]+([0-9]+).*" "\\1" db_version_minor "${db_version_str}")
  string(REGEX REPLACE ".*#define[\t ]+DB_VERSION_PATCH[\t ]+([0-9]+).*" "\\1" db_version_patch "${db_version_str}")

  set(DB_VERSION_STRING "${db_version_family}.${db_version_release}.${db_version_major}.${db_version_minor}.${db_version_patch}")
endif()

find_package_handle_standard_args(DB REQUIRED_VARS DB_LIBRARY DB_INCLUDE_DIR
                                     VERSION_VAR DB_VERSION_STRING)
if(DB_FOUND)
  set(LIZARDFS_HAVE_DB YES)
endif()
