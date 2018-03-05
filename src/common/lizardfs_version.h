/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

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

// A special version reported for disconnected chunkservers in MATOCL_CSSERV_LIST
constexpr uint32_t kDisconnectedChunkserverVersion = lizardfsVersion(256, 0, 0);

// Definitions of milestone LizardFS versions
constexpr uint32_t kStdVersion = lizardfsVersion(2, 6, 0);
constexpr uint32_t kFirstXorVersion = lizardfsVersion(2, 9, 0);
constexpr uint32_t kFirstECVersion = lizardfsVersion(3, 9, 5);
constexpr uint32_t kACL11Version = lizardfsVersion(3, 11, 0);
constexpr uint32_t kRichACLVersion = lizardfsVersion(3, 12, 0);
constexpr uint32_t kEC2Version = lizardfsVersion(3, 13, 0);
