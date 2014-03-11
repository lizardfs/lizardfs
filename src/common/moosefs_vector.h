#pragma once

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
		size_t sizeOfElement = ::serializedSize(T());
		sassert(this->size() == 0);
		sassert(sizeOfElement > 0);
		while (bytesLeftInBuffer > 0) {
			this->push_back(T());
			::deserialize(source, bytesLeftInBuffer, this->back());
		}
	}
};
