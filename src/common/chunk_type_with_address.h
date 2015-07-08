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

#include "common/chunk_type.h"
#include "common/network_address.h"
#include "common/serialization.h"

struct ChunkTypeWithAddress {
	NetworkAddress address;
	ChunkType chunkType;

	ChunkTypeWithAddress() :
		chunkType(ChunkType::getStandardChunkType()) {
	}

	ChunkTypeWithAddress(const NetworkAddress& address, const ChunkType& chunkType)
		: address(address), chunkType(chunkType) {
	}

	bool operator==(const ChunkTypeWithAddress& other) const {
		return std::make_pair(address, chunkType) == std::make_pair(other.address, other.chunkType);
	}

	bool operator<(const ChunkTypeWithAddress& other) const {
		return std::make_pair(address, chunkType) < std::make_pair(other.address, other.chunkType);
	}
};

inline uint32_t serializedSize(const ChunkTypeWithAddress& chunkTypeWithAddress) {
	return serializedSize(chunkTypeWithAddress.address, chunkTypeWithAddress.chunkType);
}

inline void serialize(uint8_t** destination, const ChunkTypeWithAddress& chunkTypeWithAddress) {
	serialize(destination, chunkTypeWithAddress.address, chunkTypeWithAddress.chunkType);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		ChunkTypeWithAddress& chunkTypeWithAddress) {
	deserialize(source, bytesLeftInBuffer, chunkTypeWithAddress.address,
			chunkTypeWithAddress.chunkType);
}
