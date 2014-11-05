#pragma once

#include "common/platform.h"

#include "common/media_label.h"
#include "common/network_address.h"
#include "common/serialization_macros.h"

SERIALIZABLE_CLASS_BEGIN(ChunkWithAddressAndLabel)
SERIALIZABLE_CLASS_BODY(ChunkWithAddressAndLabel,
		NetworkAddress, address,
		MediaLabel    , label,
		uint8_t       , reserved)

	bool operator==(const ChunkWithAddressAndLabel& other) const {
		return std::make_pair(address, label) == std::make_pair(other.address, other.label);
	}

	bool operator<(const ChunkWithAddressAndLabel& other) const {
		return std::make_pair(address, label) < std::make_pair(other.address, other.label);
	}
SERIALIZABLE_CLASS_END;
