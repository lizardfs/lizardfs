# Inspired from /usr/share/autoconf/autoconf/c.m4
# brought from: http://www.cmake.org/Wiki/CMakeTestInline

FOREACH(KEYWORD "inline" "__inline__" "__inline")
	IF(NOT DEFINED C_INLINE)
		TRY_COMPILE(C_HAS_${KEYWORD} "${CMAKE_BINARY_DIR}"
			"${CMAKE_CURRENT_SOURCE_DIR}/test_inline.cc"
			COMPILE_DEFINITIONS "-Dinline=${KEYWORD}")
		IF(C_HAS_${KEYWORD})
			SET(C_INLINE TRUE)
			ADD_DEFINITIONS("-Dinline=${KEYWORD}")
			message(STATUS "Using ${KEYWORD} as inline keyword")
		ENDIF(C_HAS_${KEYWORD})
	ENDIF(NOT DEFINED C_INLINE)
ENDFOREACH(KEYWORD)
IF(NOT DEFINED C_INLINE)
	ADD_DEFINITIONS("-Dinline=")
	message(STATUS "No form of inline keyword supported by the compiler")
ENDIF(NOT DEFINED C_INLINE)
