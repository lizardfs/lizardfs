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

function(shared_add_library NAME ...)
	list(REMOVE_AT ARGV 0)

	add_library(${NAME} STATIC ${ARGV})
	if(ENABLE_PIC_TARGETS)
		add_library(${NAME}_pic STATIC ${ARGV})
		if("${CMAKE_VERSION}" VERSION_GREATER 2.8.9)
			set_property(TARGET ${NAME}_pic PROPERTY POSITION_INDEPENDENT_CODE ON)
		else()
			set_property(TARGET ${NAME}_pic PROPERTY COMPILE_FLAGS "-fPIC")
		endif()
	endif()
endfunction()

function(shared_target_link_libraries TARGET)
	list(REMOVE_AT ARGV 0)

	set(libraries_static "")
	set(libraries_pic "")
	set(scan_mode "MIXED")
	foreach(library IN LISTS ARGV)
		if("${library}" MATCHES "STATIC|SHARED|MIXED" )
			set(scan_mode "${library}")
		elseif("${scan_mode}" STREQUAL "STATIC")
			list(APPEND libraries_static "${library}")
		elseif("${scan_mode}" STREQUAL "SHARED")
			list(APPEND libraries_pic "${library}")
		else()
			if(TARGET "${library}_pic")
				list(APPEND libraries_pic "${library}_pic")
			else()
				list(APPEND libraries_pic "${library}")
			endif()
			list(APPEND libraries_static "${library}")
		endif()
	endforeach()

	target_link_libraries(${TARGET} ${libraries_static})
	if(ENABLE_PIC_TARGETS)
		target_link_libraries(${TARGET}_pic ${libraries_pic})
	endif()
endfunction()
