#pragma once

#include <cstdint>

constexpr uint32_t lizardfsVersion(uint32_t major, uint32_t minor, uint32_t micro) {
	return 0x010000 * major + 0x0100 * minor + micro;
}
