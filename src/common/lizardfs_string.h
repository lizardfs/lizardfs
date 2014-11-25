#pragma once

#include "common/platform.h"

#include <cstring>
#include <limits>
#include <string>

#include "common/serialization.h"

/*
 * A class which represents strings which are not null-terminated when serialized.
 * The format is as follows:
 *    length:8/16/32 data:BYTES[length]
 */
template <typename LengthType>
class LizardFsString : public std::string {
public:
	template<typename... Args>
	LizardFsString(Args&&... args) : std::string(std::forward<Args>(args)...) {
	}

	static constexpr uint32_t maxLength() {
		return std::numeric_limits<LengthType>::max();
	}

	uint32_t serializedSize() const {
		sassert(length() <= maxLength());
		return sizeof(LengthType) + length();
	}

	void serialize(uint8_t** destination) const {
		sassert(length() <= maxLength());
		::serialize(destination, LengthType(length()));
		memcpy(*destination, c_str(), length());
		*destination += length();
	}

	void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer) {
		LengthType deserializedLength;
		::deserialize(source, bytesLeftInBuffer, deserializedLength);
		if (bytesLeftInBuffer < deserializedLength) {
			throw IncorrectDeserializationException("Buffer too short to deserialize string");
		}
		sassert(empty());
		*this = std::string(reinterpret_cast<const char*>(*source), deserializedLength);
		*source += deserializedLength;
		bytesLeftInBuffer -= deserializedLength;
	}
};
