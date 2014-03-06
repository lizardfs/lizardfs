#pragma once

#include "common/serialization.h"

template<class T>
class VectorSerializationWrapper {
public:
	VectorSerializationWrapper(T object) : object_(object) {
	}

	T get() const {
		return object_;
	}
private:
	T object_;
};

template<class T>
inline VectorSerializationWrapper<std::vector<T>&> makeSerializationWrapper(std::vector<T>& vector) {
	return VectorSerializationWrapper<std::vector<T>&>(vector);
}

template<class T>
inline VectorSerializationWrapper<const std::vector<T>&> makeSerializationWrapper(
		const std::vector<T>& vector) {
	return VectorSerializationWrapper<const std::vector<T>&>(vector);
}

template<class T>
inline uint32_t serializedSize(const VectorSerializationWrapper<T>& vector) {
	uint32_t ret = 0;
	ret += serializedSize(uint32_t(vector.get().size()));
	for (const auto& element : vector.get()) {
		ret += serializedSize(element);
	}
	return ret;
}

template<class T>
inline void serialize(uint8_t** destination, const VectorSerializationWrapper<T>& vector) {
	serialize(destination, uint32_t(vector.get().size()));
	for (const auto& element : vector.get()) {
		serialize(destination, element);
	}
}

template<class T>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		VectorSerializationWrapper<T>& vector) {
	sassert(vector.get().size() == 0);
	uint32_t size;
	deserialize(source, bytesLeftInBuffer, size);
	vector.get().resize(size);
	for (uint32_t i = 0; i < size; ++i) {
		deserialize(source, bytesLeftInBuffer, vector.get()[i]);
	}
}
