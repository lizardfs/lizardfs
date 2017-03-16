# Finds Thrift
# Input:
#     THRIFT_ROOT -- Thrift installation prefix (optional)
# Output:
#     THRIFT_FOUND
#     THRIFT_INCLUDE_DIRS
#     THRIFT_LIBRARIES
#     THRIFT_COMPILER

# Dependencies:
if (NOT Boost_INCLUDE_DIRS)
	if(Thrift_FIND_REQUIRED)
		message(FATAL_ERROR "Thrift requires Boost headers")
	elseif(NOT Thrift_FIND_QUIETLY)
		message(STATUS "Thrift requires Boost headers")
	endif()
	return()
endif()

# Find all possible components, ie. library and compiler
find_path(THRIFT_INCLUDE_DIR thrift/Thrift.h HINTS
	$ENV{THRIFT_ROOT}/include
	${THRIFT_ROOT}/include
	/opt/thrift-0.9.1/include
	/opt/thrift-0.9.0/include)
mark_as_advanced(THRIFT_INCLUDE_DIR)

find_library(THRIFT_LIBRARY thrift HINTS ${THRIFT_INCLUDE_DIR}/../lib)
mark_as_advanced(THRIFT_LIBRARY)

find_program(THRIFT_COMPILER thrift HINTS
	${THRIFT_INCLUDE_DIR}/../bin
	$ENV{THRIFT_ROOT}/bin
	${THRIFT_ROOT}/bin
	/opt/thrift-0.9.1/bin
	/opt/thrift-0.9.0/bin)
mark_as_advanced(THRIFT_COMPILER)

# Call find_package_handle_standard_args for components required in find_package(Thrift)
set(REQUIRED_ITEMS "")
foreach(COMPONENT ${Thrift_FIND_COMPONENTS})
	if (COMPONENT STREQUAL "library")
		set(REQUIRED_ITEMS ${REQUIRED_ITEMS} "THRIFT_INCLUDE_DIR" "THRIFT_LIBRARY")
	elseif(COMPONENT STREQUAL "compiler")
		set(REQUIRED_ITEMS ${REQUIRED_ITEMS} "THRIFT_COMPILER")
	else()
		message(FATAL_ERROR "Requested an unknown Thrift component '${COMPONENT}'")
	endif()
endforeach()
if (REQUIRED_ITEMS STREQUAL "")
	set(REQUIRED_ITEMS "THRIFT_INCLUDE_DIR" "THRIFT_LIBRARY")
endif()
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Thrift DEFAULT_MSG ${REQUIRED_ITEMS})

if (THRIFT_INCLUDE_DIR)
	set(THRIFT_INCLUDE_DIRS ${THRIFT_INCLUDE_DIR} ${Boost_INCLUDE_DIRS})
endif()
if (THRIFT_LIBRARY)
	set(THRIFT_LIBRARIES ${THRIFT_LIBRARY})
endif()
