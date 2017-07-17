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

#include <tuple>

#include "common/chunk_part_type.h"
#include "common/media_label.h"
#include "common/network_address.h"
#include "common/serialization_macros.h"

namespace legacy {

SERIALIZABLE_CLASS_BEGIN(ChunkPartWithAddressAndLabel)
SERIALIZABLE_CLASS_BODY(ChunkPartWithAddressAndLabel,
		NetworkAddress, address,
		std::string   , label,
		ChunkPartType , chunkType)

	bool operator==(const ChunkPartWithAddressAndLabel& other) const {
		return std::make_tuple(address, label, chunkType)
				== std::make_tuple(other.address, other.label, other.chunkType);
	}

	bool operator<(const ChunkPartWithAddressAndLabel& other) const {
		return std::make_tuple(address, label, chunkType)
				< std::make_tuple(other.address, other.label, other.chunkType);
	}
SERIALIZABLE_CLASS_END;

} // legacy

SERIALIZABLE_CLASS_BEGIN(ChunkPartWithAddressAndLabel)
SERIALIZABLE_CLASS_BODY(ChunkPartWithAddressAndLabel,
		NetworkAddress, address,
		std::string   , label,
		ChunkPartType , chunkType)

	bool operator==(const ChunkPartWithAddressAndLabel& other) const {
		return std::make_tuple(address, label, chunkType)
				== std::make_tuple(other.address, other.label, other.chunkType);
	}

	bool operator<(const ChunkPartWithAddressAndLabel& other) const {
		return std::make_tuple(address, label, chunkType)
				< std::make_tuple(other.address, other.label, other.chunkType);
	}
SERIALIZABLE_CLASS_END;

SERIALIZABLE_CLASS_BEGIN(ChunkWithAddressAndLabel)
SERIALIZABLE_CLASS_BODY(ChunkWithAddressAndLabel,
	uint64_t, chunk_id,
	uint32_t, chunk_version,
	std::vector<ChunkPartWithAddressAndLabel>, chunk_parts)
SERIALIZABLE_CLASS_END;
