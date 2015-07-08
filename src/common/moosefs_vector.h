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

#include "common/serialization.h"

// This class behaves just as std::vector, with the exception that it is serialized
// differently. MooseFS does not send array length when serializing it, LizardFS does.
template <class T>
class MooseFSVector : public std::vector<T> {
public:
	// Gcc 4.6 which we support don't support inherited constructors,
	// so a workaround was needed:
	template<typename... Args>
	MooseFSVector(Args&&... args) : std::vector<T>(std::forward<Args>(args)...) {
	}

	uint32_t serializedSize() const {
		uint32_t ret = 0;
		for (const auto& element : *this) {
			ret += ::serializedSize(element);
		}
		return ret;
	}

	void serialize(uint8_t** destination) const {
		for (const T& t : *this) {
			::serialize(destination, t);
		}
	}

	void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer) {
		sassert(this->empty());
		while (bytesLeftInBuffer > 0) {
			uint32_t prevBytesLeftInBuffer = bytesLeftInBuffer;
			this->emplace_back();
			::deserialize(source, bytesLeftInBuffer, this->back());
			sassert(bytesLeftInBuffer < prevBytesLeftInBuffer);
		}
	}
};
