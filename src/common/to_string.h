#pragma once
#include "config.h"

#ifdef __CYGWIN__

#include <sstream>

template <class C>
std::string toString(C value) {
	std::stringstream ss;
	ss << value;
	return ss.str();
}

#else // !__CYGWIN__

#include <string>

template <class C>
std::string toString(C value) {
	return std::to_string(value);
}

#endif // __CYGWIN__
