#  Copyright 2015-2017 Skytechnology sp. z o.o..
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

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

collect_sources(MOUNT)

shared_add_library(mount ${MOUNT_SOURCES})
shared_target_link_libraries(mount mfscommon ${ADDITIONAL_LIBS})

create_unittest(mount ${MOUNT_TESTS})
link_unittest(mount mount mfscommon)

if (ENABLE_CLIENT_LIB)
  add_subdirectory(client)
endif()
