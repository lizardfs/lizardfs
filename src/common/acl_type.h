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

#include <string>

#include "common/hashfn.h"
#include "common/serialization.h"

enum class AclType : uint8_t { kAccess, kDefault, kRichACL };

static inline void hashCombineRaw(uint64_t& seed, AclType hash) {
	return hashCombineRaw(seed, uint64_t(hash));
}

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
	case static_cast<uint8_t>(AclType::kRichACL):
			val = AclType::kRichACL;
			break;
	default: throw IncorrectDeserializationException(
			"Deserialized malformed value of AclType: " + std::to_string(deserializedValue));
	}
}
