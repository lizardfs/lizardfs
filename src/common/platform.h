#pragma once
#include "config.h"

/* Workaround for Debian/kFreeBSD which does not define ENODATA */
#if defined(__FreeBSD_kernel__)
#ifndef ENODATA
#define ENODATA ENOATTR
#endif
#endif

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

#ifndef LIZARDFS_HAVE_STD_STOULL

#include <cstdlib>
#include <string>

namespace std {

inline unsigned long long stoull(const std::string& s, std::size_t* pos = 0, int base = 10) {
	char* end = nullptr;
	unsigned long long val = strtoull(s.c_str(), &end, base);
	if (pos != nullptr) {
		*pos = end - s.c_str();
	}
	return val;
}

}

#endif /* #ifndef LIZARDFS_HAVE_STD_STOULL */
