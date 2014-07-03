#pragma once
#include "config.h"

#ifndef LIZARDFS_HAVE_STD_TO_STRING

#include <sstream>
#include <string>

namespace std {

template<typename T>
inline std::string to_string(const T& val) {
	std::stringstream ss;
	ss << val;
	return ss.str();
}

}

#endif /* #ifndef LIZARDFS_HAVE_STD_TO_STRING */
