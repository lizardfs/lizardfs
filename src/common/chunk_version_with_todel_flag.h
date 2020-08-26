/*
   Copyright 2020 Skytechnology sp. z o.o.

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

namespace common {

using chunk_version_t = uint32_t;

/**
 * Mask to set/get todel flag in/from version filed of chunks' info sent in
 * cstoma::chunkNew and cstoma::registerChunks packages.
 */
constexpr chunk_version_t TODEL_MASK = 0x80000000;
/**
 * Mask to set/get actual chunk version in/from version filed of chunks' info sent in
 * cstoma::chunkNew and cstoma::registerChunks packages.
 */
constexpr chunk_version_t VERSION_MASK = 0x7FFFFFFF;

/** \brief Combine \p chunkVersion and \p todel flag into a single value.
 */
constexpr chunk_version_t combineVersionWithTodelFlag(chunk_version_t chunkVersion, bool todel) noexcept {
	return (chunkVersion | (todel ? TODEL_MASK : 0));
}

constexpr chunk_version_t getChunkVersion(chunk_version_t versionWithTodelFlag) noexcept {
	return (versionWithTodelFlag & VERSION_MASK);
}

constexpr bool getTodelFlag(chunk_version_t versionWithTodelFlag) noexcept {
	return (versionWithTodelFlag & TODEL_MASK);
}

} // namespace common
