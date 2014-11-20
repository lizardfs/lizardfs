#pragma once

#include "common/platform.h"

#include <cstdint>
#include <sstream>

#define LIZARDFS_VERSION(major, minor, micro) (0x010000 * major + 0x0100 * minor + micro)

constexpr uint32_t lizardfsVersion(uint32_t major, uint32_t minor, uint32_t micro) {
	return 0x010000 * major + 0x0100 * minor + micro;
}

inline std::string lizardfsVersionToString(uint32_t version) {
	std::ostringstream ss;
	ss << version / 0x010000 << '.' << (version % 0x010000) / 0x0100 << '.' << version % 0x0100;
	return ss.str();
}

// Definitions of milestone LizardFS versions
constexpr uint32_t kFirstXorVersion = lizardfsVersion(2, 9, 0);
