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

collect_sources(NFS_GANESHA_PLUGIN)

include_directories(${CMAKE_SOURCE_DIR}/external/${NFS_GANESHA_DIR_NAME}/src/include)
include_directories(${CMAKE_SOURCE_DIR}/external/${NTIRPC_DIR_NAME}/ntirpc)

add_library(fsallizardfs MODULE ${NFS_GANESHA_PLUGIN_MAIN} ${NFS_GANESHA_PLUGIN_SOURCES})

target_link_libraries(fsallizardfs lizardfs-client_pic)

install(TARGETS fsallizardfs COMPONENT fsal DESTINATION ${LIB_SUBDIR}/ganesha )
