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
class MooseFsString : public std::string {
public:
	template<typename... Args>
	MooseFsString(Args&&... args) : std::string(std::forward<Args>(args)...) {
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
