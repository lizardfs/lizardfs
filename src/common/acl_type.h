#pragma once

#include "config.h"

#include <string>

#include "common/serialization.h"
#include "common/to_string.h"

enum class AclType : uint8_t { kAccess, kDefault };

// TODO(msulikowski) think of some macros which would automatically generate
// such a code for enum classes

inline uint32_t serializedSize(const AclType& val) {
	return ::serializedSize(static_cast<uint8_t>(val));
}

inline void serialize(uint8_t** destination, const AclType& val) {
	::serialize(destination, static_cast<uint8_t>(val));
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, AclType& val) {
	uint8_t deserializedValue;
	::deserialize(source, bytesLeftInBuffer, deserializedValue);

	switch (deserializedValue) {
	case static_cast<uint8_t>(AclType::kDefault):
			val = AclType::kDefault;
			break;
	case static_cast<uint8_t>(AclType::kAccess):
			val = AclType::kAccess;
			break;
	default: throw IncorrectDeserializationException(
			"Deserialized malformed value of AclType: " + toString(deserializedValue));
	}
}
