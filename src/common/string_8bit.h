#pragma once

#include <cstring>
#include <string>

#include "common/serialization.h"

/*
 * A class which represents short strings which are used in some MooseFS network messages
 * in the following format:
 *    length:8 data:BYTES[length]
 */
class String8Bit : public std::string {
public:
	template<typename... Args>
	String8Bit(Args&&... args) : std::string(std::forward<Args>(args)...) {
	}

	static constexpr uint32_t kMaxLength = UINT8_MAX;

	uint32_t serializedSize() const {
		sassert(length() <= kMaxLength);
		return 1 + length();
	}

	void serialize(uint8_t** destination) const {
		sassert(length() <= kMaxLength);
		::serialize(destination, uint8_t(length()));
		memcpy(*destination, c_str(), length());
		*destination += length();
	}

	void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer) {
		uint8_t deserializedLength;
		::deserialize(source, bytesLeftInBuffer, deserializedLength);
		if (bytesLeftInBuffer < deserializedLength) {
			throw IncorrectDeserializationException("Buffer too short to deserialize String8Bit");
		}
		sassert(empty());
		*this = std::string(reinterpret_cast<const char*>(*source), deserializedLength);
		*source += deserializedLength;
		bytesLeftInBuffer -= deserializedLength;
	}
};
