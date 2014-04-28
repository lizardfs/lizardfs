#pragma once

#include "config.h"

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
