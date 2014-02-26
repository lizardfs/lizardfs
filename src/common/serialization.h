#pragma once

#include <vector>

#include "common/datapack.h"
#include "common/exception.h"
#include "common/massert.h"

/*
 * Exception thrown on deserialization error
 */
class IncorrectDeserializationException : public Exception {
public:
	IncorrectDeserializationException(const std::string& message):
			Exception("Deserialization error: " + message) {}
};

// serializedSize

inline uint32_t serializedSize(const bool&) {
	return 1;
}

inline uint32_t serializedSize(const uint8_t&) {
	return 1;
}

inline uint32_t serializedSize(const uint16_t&) {
	return 2;
}

inline uint32_t serializedSize(const uint32_t&) {
	return 4;
}

inline uint32_t serializedSize(const uint64_t&) {
	return 8;
}

inline uint32_t serializedSize(const char& c) {
	return serializedSize(reinterpret_cast<const uint8_t&>(c));
}

template <class T, int N>
inline uint32_t serializedSize(const T (&array)[N]) {
	return N * serializedSize(array[0]);
}

template<class T1, class T2>
inline uint32_t serializedSize(const std::pair<T1, T2>& pair) {
	return serializedSize(pair.first) + serializedSize(pair.second);
}

inline uint32_t serializedSize(const std::string& value) {
	return serializedSize(value.size()) + serializedSize(std::string::value_type()) * value.size();
}

inline uint32_t serializedSize(const std::vector<std::string>& vector) {
	uint32_t ret = 0;
	ret += serializedSize(vector.size());
	for (const auto& str : vector) {
		ret += serializedSize(str);
	}
	return ret;
}

template<class T>
inline uint32_t serializedSize(const std::vector<T>& vector) {
	return vector.size() * serializedSize(T());
}

template<class T, class ... Args>
inline uint32_t serializedSize(const T& t, const Args& ... args) {
	return serializedSize(t) + serializedSize(args...);
}

// serialize for simple types

// serialize bool
inline void serialize(uint8_t** destination, const bool& value) {
	put8bit(destination, static_cast<uint8_t>(value ? 1 : 0));
}

// serialize uint8_t
inline void serialize(uint8_t** destination, const uint8_t& value) {
	put8bit(destination, value);
}

// serialize uint16_t
inline void serialize(uint8_t** destination, const uint16_t& value) {
	put16bit(destination, value);
}

// serialize uint32_t
inline void serialize(uint8_t** destination, const uint32_t& value) {
	put32bit(destination, value);
}

// serialize uint64_t
inline void serialize(uint8_t** destination, const uint64_t& value) {
	put64bit(destination, value);
}

inline void serialize(uint8_t** destination, const char& value) {
	serialize(destination, reinterpret_cast<const uint8_t&>(value));
}

// serialize fixed size array ("type name[number];")
template <class T, int N>
inline void serialize(uint8_t** destination, const T (&array)[N]) {
	for (int i = 0; i < N; i++) {
		serialize(destination, array[i]);
	}
}

// serialize a pair
template<class T1, class T2>
inline void serialize(uint8_t** destination, const std::pair<T1, T2>& pair) {
	serialize(destination, pair.first);
	serialize(destination, pair.second);
}

inline void serialize(uint8_t** destination, const std::string& value) {
	serialize(destination, value.length());
	for (unsigned i = 0; i < value.length(); ++i) {
		serialize(destination, value[i]);
	}
}

inline void serialize(uint8_t** destination, const std::vector<std::string>& vector) {
	serialize(destination, vector.size());
	for (const auto& str : vector) {
		serialize(destination, str);
	}
}

// serialize a vector
template<class T>
inline void serialize(uint8_t** destination, const std::vector<T>& vector) {
	for (const T& t : vector) {
		serialize(destination, t);
	}
}

template<class T, class... Args>
inline void serialize(uint8_t** destination, const T& t, const Args&... args) {
	serialize(destination, t);
	serialize(destination, args...);
}

// helpers

template <class T>
inline void verifySize(const T& value, uint32_t bytesLeft) {
	if (bytesLeft < serializedSize(value)) {
		throw IncorrectDeserializationException("unexpected end of buffer");
	}
}

// deserialize functions for simple types

// deserialize bool
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, bool& value) {
	verifySize(value, bytesLeftInBuffer);
	bytesLeftInBuffer -= 1;
	uint8_t integerValue = get8bit(source);
	if (integerValue > 1) {
		throw IncorrectDeserializationException("corrupted boolean value");
	}
	value = static_cast<bool>(integerValue);
}

// deserialize uint8_t
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, uint8_t& value) {
	verifySize(value, bytesLeftInBuffer);
	bytesLeftInBuffer -= 1;
	value = get8bit(source);
}

// deserialize uint16_t
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, uint16_t& value) {
	verifySize(value, bytesLeftInBuffer);
	bytesLeftInBuffer -= 2;
	value = get16bit(source);
}

// deserialize uint32_t
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, uint32_t& value) {
	verifySize(value, bytesLeftInBuffer);
	bytesLeftInBuffer -= 4;
	value = get32bit(source);
}

// deserialize uint64_t
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, uint64_t& value) {
	verifySize(value, bytesLeftInBuffer);
	bytesLeftInBuffer -= 8;
	value = get64bit(source);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, char& value) {
	deserialize(source, bytesLeftInBuffer, reinterpret_cast<uint8_t&>(value));
}

// deserialize fixed size array ("type name[number];")
template <class T, int N>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, T (&array)[N]) {
	for (int i = 0; i < N; i++) {
		deserialize(source, bytesLeftInBuffer, array[i]);
	}
}

// deserialize a pair
template<class T1, class T2>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		std::pair<T1, T2>& pair) {
	deserialize(source, bytesLeftInBuffer, pair.first);
	deserialize(source, bytesLeftInBuffer, pair.second);
}

// deserialize uint8_t* (as a pointer to the serialized data)
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, const uint8_t*& value) {
	if (bytesLeftInBuffer == 0) {
		throw IncorrectDeserializationException("unexpected end of buffer");
	}
	bytesLeftInBuffer = 0;
	value = *source;
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, std::string& value) {
	sassert(value.size() == 0);
	uint32_t size;
	deserialize(source, bytesLeftInBuffer, size);
	value.resize(size);
	for (unsigned i = 0; i < size; ++i) {
		deserialize(source, bytesLeftInBuffer, value[i]);
	}
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		std::vector<std::string>& vec) {
	sassert(vec.size() == 0);
	uint32_t size;
	deserialize(source, bytesLeftInBuffer, size);
	vec.resize(size);
	for (unsigned i = 0; i < size; ++i) {
		deserialize(source, bytesLeftInBuffer, vec[i]);
	}
}

// deserialize a vector
template<class T>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		std::vector<T>& vector) {
	size_t sizeOfElement = serializedSize(T());
	sassert(vector.size() == 0);
	sassert(sizeOfElement > 0);
	if (bytesLeftInBuffer % sizeOfElement != 0) {
		throw IncorrectDeserializationException(
				"vector: buffer size not divisible by element size");
	}
	size_t vecSize = bytesLeftInBuffer / sizeOfElement;
	vector.resize(vecSize);
	for (size_t i = 0; i < vecSize; ++i) {
		deserialize(source, bytesLeftInBuffer, vector[i]);
	}
}

template<class T, class... Args>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, T& t, Args&... args) {
	deserialize(source, bytesLeftInBuffer, t);
	deserialize(source, bytesLeftInBuffer, args...);
}

/*
 * Advances deserialization state without reading data
 */
template <class T>
inline void deserializeAndIgnore(const uint8_t** source, uint32_t& bytesLeftInBuffer) {
	verifySize(T(), bytesLeftInBuffer);
	*source += serializedSize(T());
	bytesLeftInBuffer -= serializedSize(T());
}

/*
 * The main interface of the serialization framework
 */

/*
 * Serializes a tuple of variables into the buffer.
 * The buffer must be empty when calling this function and will be properly resized
 */
template <class... Args>
void serialize(std::vector<uint8_t>& buffer, const Args&... args) {
	sassert(buffer.empty());
	buffer.resize(serializedSize(args...));
	uint8_t* destination = buffer.data();
	serialize(&destination, args...);
	sassert(std::distance(buffer.data(), destination) == (int32_t)buffer.size());
}

/*
 * Deserializes a tuple of variables from the data in the sourceBuffer.
 * Throws IncorrectDeserializationException when buffer is too short or malformed (some types
 * may check if the input bytes represent an acceptable value).
 * Returns number of bytes that were not used in the deserialization process (ie. this value
 * is greater than zero is the buffer is longer than size of all the deserialized data).
 */
template<class... Args>
inline uint32_t deserialize(const uint8_t* sourceBuffer, uint32_t sourceBufferSize, Args&... args) {
	deserialize(&sourceBuffer, sourceBufferSize, args...);
	return sourceBufferSize;
}

/*
 * The same as the function above, but with the std::vector interface
 */
template<class... Args>
inline uint32_t deserialize(const std::vector<uint8_t>& sourceBuffer, Args&... args) {
	return deserialize(sourceBuffer.data(), sourceBuffer.size(), args...);
}
