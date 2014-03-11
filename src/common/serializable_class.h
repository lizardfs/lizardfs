#pragma once

#include <cinttypes>

#include "common/serialization.h"
#include "common/serializable_class_generated.h"

// Macros used to concatenate two macro names:
#define PASTE(a,b) a ## b
#define PASTE_B(a,b) PASTE(a,b)

#define APPLY(M, ...) M(__VA_ARGS__)

// A macro that calls 'Macro' for every variadic parameter and joins results with a separator
// Implemented with use of auto-generated APPLY1_<N>
#define APPLY1(Macro, Sep, ...) APPLY(PASTE_B(APPLY1_, COUNT_ARGS(__VA_ARGS__)), Macro, Sep,__VA_ARGS__)

// A macro that calls 'Macro' for every pair of 2 consequitive variadic parameters
// and joins results with a separator. Implemented with use of auto-generated APPLY2_<N>
#define APPLY2(Macro, Sep, ...) APPLY(PASTE_B(APPLY2_, COUNT_ARGS(__VA_ARGS__)), Macro, Sep, __VA_ARGS__)

// A macro that converts a list [T1, t1, T2, t2, ...] into [t1, t2, ...]
#define VARS_COMMAS(...) APPLY(PASTE_B(VARS_COMMAS_, COUNT_ARGS(__VA_ARGS__)), __VA_ARGS__)

// Aliases that allow to use other macro as a call parameter:
#define APPLY1_B(...) APPLY1(__VA_ARGS__)
#define APPLY2_B(...) APPLY2(__VA_ARGS__)

// Some simple macros usefull when defining a class:
#define PARAMETER(t) t ## _
#define DECLARE(T, t) T PARAMETER(t)
#define CONST_REF(T, t) const T& t
#define REFERENCE(T, t) T& t
#define INITIALIZE(t) PARAMETER(t) (t)

// A workaround to pass a semicolon and a comma as a [semi-]recursive macro parameter:
#define MAKE_SEMICOLON() ;
#define MAKE_COMMA() ,

// Class variables declaration:
#define DECLARE_VARIABLES(ClassName, ...) \
	APPLY2(DECLARE, MAKE_SEMICOLON, __VA_ARGS__);

// Tuple-like constructor:
#define TUPLE_LIKE_CONSTRUCTOR(ClassName, ...) \
	ClassName(APPLY2(CONST_REF, MAKE_COMMA, __VA_ARGS__)) \
		: APPLY1_B(INITIALIZE, MAKE_COMMA, VARS_COMMAS(__VA_ARGS__)) { \
	};

// Methods used for serialization:
#define SERIALIZE_METHODS(ClassName, ...) \
	uint32_t serializedSize() const { \
		return ::serializedSize( \
			APPLY1_B(PARAMETER, MAKE_COMMA, VARS_COMMAS(__VA_ARGS__))); \
	} \
	void serialize(uint8_t** destination) const { \
		return ::serialize(destination, \
			APPLY1_B(PARAMETER, MAKE_COMMA, VARS_COMMAS(__VA_ARGS__))); \
	} \
	void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer) { \
		return ::deserialize(source, bytesLeftInBuffer, \
			APPLY1_B(PARAMETER, MAKE_COMMA, VARS_COMMAS(__VA_ARGS__))); \
	}

// Set of macros used to generate a serializable class:

// A header is a class name, potentially followed by a list of ancestors, like 'Der : public Base'
#define SERIALIZABLE_CLASS_BEGIN(ClassHeader) \
struct ClassHeader {

// A name is a repeated class name used in SERIALIZABLE_CLASS_BEGIN parameter
#define SERIALIZABLE_CLASS_BODY(ClassName, ...) \
	TUPLE_LIKE_CONSTRUCTOR(ClassName, __VA_ARGS__) \
	DECLARE_VARIABLES(ClassName, __VA_ARGS__) \
	SERIALIZE_METHODS(ClassName, __VA_ARGS__)

#define SERIALIZABLE_CLASS_END }
