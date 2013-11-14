#ifndef LIZARDFS_COMMON_SERIALIZATION_H_
#define LIZARDFS_COMMON_SERIALIZATION_H_

#include <exception>
#include <vector>

#include "common/datapack.h"
#include "common/massert.h"

/*
 * Exception thrown on deserialization error
 */
class IncorrectDeserializationException : public std::exception {
};

// serializedSize

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

template<class T>
inline uint32_t serializedSize(const std::vector<T>& vec) {
	return vec.size() * serializedSize(T());
}

template<class T, class ... Args>
inline uint32_t serializedSize(const T& t, const Args& ... args) {
	return serializedSize(t) + serializedSize(args...);
}

// serialize for simple types

inline void serialize(uint8_t** destination, const uint8_t& value) {
	put8bit(destination, value);
}

inline void serialize(uint8_t** destination, const uint16_t& value) {
	put16bit(destination, value);
}

inline void serialize(uint8_t** destination, const uint32_t& value) {
	put32bit(destination, value);
}

inline void serialize(uint8_t** destination, const uint64_t& value) {
	put64bit(destination, value);
}

template<class T>
inline void serialize(uint8_t** destination, const std::vector<T>& vec) {
	for (const T& t : vec) {
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
		throw IncorrectDeserializationException();
	}
}

// deserialize functions for simple types

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

// deserialize a vector
template<class T>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, std::vector<T>& vec) {
	size_t sizeOfElement = serializedSize(T());
	sassert(vec.size() == 0);
	sassert(sizeOfElement > 0);
	if (bytesLeftInBuffer % sizeOfElement != 0) {
		throw IncorrectDeserializationException();
	}
	size_t vecSize = bytesLeftInBuffer / sizeOfElement;
	vec.resize(vecSize);
	for (size_t i = 0; i < vecSize; ++i) {
		deserialize(source, bytesLeftInBuffer, vec[i]);
	}
}

// deserialize uint8_t* (as a pointer to the serialized data)
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, const uint8_t*& value) {
	if (bytesLeftInBuffer == 0) {
		throw IncorrectDeserializationException();
	}
	bytesLeftInBuffer = 0;
	value = *source;
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

#endif // LIZARDFS_COMMON_SERIALIZATION_H_
