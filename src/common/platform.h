#pragma once
#include "config.h"

#ifndef HAVE_STD_TO_STRING

#include <string>
#include <stringstream>

namespace std {

template<typename T>
inline std::string to_string(const T& val) {
	std::stringstream ss;
	ss << val;
	return ss.str();
}

}

#endif /* #ifndef HAVE_STD_TO_STRING */
