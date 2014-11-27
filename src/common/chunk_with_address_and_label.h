#pragma once

#include "common/platform.h"

#include <tuple>

#include "common/chunk_type.h"
#include "common/media_label.h"
#include "common/network_address.h"
#include "common/serialization_macros.h"

SERIALIZABLE_CLASS_BEGIN(ChunkWithAddressAndLabel)
SERIALIZABLE_CLASS_BODY(ChunkWithAddressAndLabel,
		NetworkAddress, address,
		MediaLabel    , label,
		ChunkType     , chunkType)

	bool operator==(const ChunkWithAddressAndLabel& other) const {
		return std::make_tuple(address, label, chunkType)
				== std::make_tuple(other.address, other.label, other.chunkType);
	}

	bool operator<(const ChunkWithAddressAndLabel& other) const {
		return std::make_tuple(address, label, chunkType)
				< std::make_tuple(other.address, other.label, other.chunkType);
	}
SERIALIZABLE_CLASS_END;
