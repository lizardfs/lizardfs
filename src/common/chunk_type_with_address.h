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

#include "common/chunk_part_type.h"
#include "common/network_address.h"
#include "common/serialization_macros.h"
#include "common/slice_traits.h"

namespace legacy {

struct ChunkTypeWithAddress {
	NetworkAddress address;
	ChunkPartType chunkType;

	ChunkTypeWithAddress() :
		chunkType() {
	}

	ChunkTypeWithAddress(const NetworkAddress& address, const ChunkPartType& chunkType)
		: address(address), chunkType(chunkType) {
	}

	bool operator==(const ChunkTypeWithAddress& other) const {
		return std::make_pair(address, chunkType) == std::make_pair(other.address, other.chunkType);
	}

	bool operator<(const ChunkTypeWithAddress& other) const {
		return std::make_pair(address, chunkType) < std::make_pair(other.address, other.chunkType);
	}

	LIZARDFS_DEFINE_SERIALIZE_METHODS(address, chunkType);
};

} // legacy

struct ChunkTypeWithAddress {
	NetworkAddress address;
	ChunkPartType chunk_type;
	uint32_t chunkserver_version;

	ChunkTypeWithAddress() :
		address(), chunk_type(), chunkserver_version() {
	}

	ChunkTypeWithAddress(const NetworkAddress& address, const ChunkPartType& chunk_type,
			uint32_t chunkserver_version)
		: address(address), chunk_type(chunk_type), chunkserver_version(chunkserver_version) {
	}

	// ChunkType is uniquely identified by the IP, port and type.
	// The chunkserver_version is needed for serializing data.
	// Adding it to compare may break logic, see counting crcErrors in ChunkReader.
	bool operator==(const ChunkTypeWithAddress& other) const {
		return std::make_tuple(address, chunk_type)
		    == std::make_tuple(other.address, other.chunk_type);
	}

	// ChunkType is uniquely identified by the IP, port and type.
	// The chunkserver_version is needed for serializing data.
	// Adding it to compare may break logic, see counting crcErrors in ChunkReader.
	bool operator<(const ChunkTypeWithAddress& other) const {
		return std::make_tuple(address, chunk_type)
		    < std::make_tuple(other.address, other.chunk_type);
	}

	LIZARDFS_DEFINE_SERIALIZE_METHODS(address, chunk_type, chunkserver_version);
};
