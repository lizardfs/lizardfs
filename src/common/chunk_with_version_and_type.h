/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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
#include <iomanip>
#include <sstream>
#include <tuple>

#include "common/chunk_part_type.h"
#include "common/serialization.h"
#include "common/slice_traits.h"

struct ChunkWithVersionAndType {
	uint64_t id;
	uint32_t version;
	ChunkPartType type;

	ChunkWithVersionAndType() : id(0), version(0), type(slice_traits::standard::ChunkPartType()) {}

	ChunkWithVersionAndType(uint64_t id, uint32_t version, ChunkPartType type)
			: id(id),
			  version(version),
			  type(type) {
	}

	std::string toString() const {
		std::stringstream ss;
		ss << std::hex << std::setfill('0');
		ss << std::setw(16) << id << '_';
		ss << std::setw(8) << version;
		ss << " (" << type.toString() << ")";
		return ss.str();
	}

	bool operator<(const ChunkWithVersionAndType& other) const {
		return std::make_tuple(id, version, type)
			< std::make_tuple(other.id, other.version, other.type);
	}
	bool operator==(const ChunkWithVersionAndType& other) const {
		return id == other.id && version == other.version && type == other.type;
	}
};

inline uint32_t serializedSize(const ChunkWithVersionAndType& chunk) {
	return serializedSize(chunk.id, chunk.version, chunk.type);
}

inline void serialize(uint8_t** destination, const ChunkWithVersionAndType& chunk) {
	return serialize(destination, chunk.id, chunk.version, chunk.type);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		ChunkWithVersionAndType& chunk) {
	return deserialize(source, bytesLeftInBuffer, chunk.id, chunk.version, chunk.type);
}
