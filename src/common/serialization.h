#pragma once

#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/datapack.h"
#include "common/exception.h"
#include "common/massert.h"

const uint32_t kMaxDeserializedBytesCount = 32 * 1024 * 1024;  // 32MiB
const uint32_t kMaxDeserializedElementsCount = 1000 * 1000;    // 1M

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

inline uint32_t serializedSize(const int8_t&) {
	return 1;
}

inline uint32_t serializedSize(const int16_t&) {
	return 2;
}

inline uint32_t serializedSize(const int32_t&) {
	return 4;
}

inline uint32_t serializedSize(const int64_t&) {
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
	return serializedSize(uint32_t(value.size())) + value.size() + 1;
}

template<class T>
inline uint32_t serializedSize(const std::unique_ptr<T>& ptr) {
	if (ptr) {
		return serializedSize(true) + serializedSize(*ptr);
	} else {
		return serializedSize(false);
	}
}

template<class T>
inline uint32_t serializedSize(const std::vector<T>& vector) {
	uint32_t ret = 0;
	ret += serializedSize(uint32_t(vector.size()));
	for (const auto& t : vector) {
		ret += serializedSize(t);
	}
	return ret;
}

template<class T>
inline uint32_t serializedSize(const T& t) {
	return t.serializedSize();
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

inline void serialize(uint8_t** destination, const int8_t& value) {
	put8bit(destination, value);
}

inline void serialize(uint8_t** destination, const int16_t& value) {
	put16bit(destination, value);
}

inline void serialize(uint8_t** destination, const int32_t& value) {
	put32bit(destination, value);
}

inline void serialize(uint8_t** destination, const int64_t& value) {
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

// serialize a string
inline void serialize(uint8_t** destination, const std::string& value) {
	serialize(destination, uint32_t(value.length() + 1));
	memcpy(*destination, value.data(), value.length());
	*destination += value.length();
	serialize(destination, char(0));
}

// serialize a unique_ptr
template<class T>
inline void serialize(uint8_t** destination, const std::unique_ptr<T>& ptr) {
	if (ptr) {
		serialize(destination, true);
		serialize(destination, *ptr);
	} else {
		serialize(destination, false);
	}
}

// serialize a vector
template<class T>
inline void serialize(uint8_t** destination, const std::vector<T>& vector) {
	serialize(destination, uint32_t(vector.size()));
	for (const T& t : vector) {
		serialize(destination, t);
	}
}

// serialization
template<class T>
inline void serialize(uint8_t** destination, const T& t) {
	return t.serialize(destination);
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

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, bool& value) {
	verifySize(value, bytesLeftInBuffer);
	bytesLeftInBuffer -= 1;
	uint8_t integerValue = get8bit(source);
	if (integerValue > 1) {
		throw IncorrectDeserializationException("corrupted boolean value");
	}
	value = static_cast<bool>(integerValue);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, uint8_t& value) {
	verifySize(value, bytesLeftInBuffer);
	bytesLeftInBuffer -= 1;
	value = get8bit(source);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, uint16_t& value) {
	verifySize(value, bytesLeftInBuffer);
	bytesLeftInBuffer -= 2;
	value = get16bit(source);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, uint32_t& value) {
	verifySize(value, bytesLeftInBuffer);
	bytesLeftInBuffer -= 4;
	value = get32bit(source);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, uint64_t& value) {
	verifySize(value, bytesLeftInBuffer);
	bytesLeftInBuffer -= 8;
	value = get64bit(source);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, int8_t& value) {
	verifySize(value, bytesLeftInBuffer);
	bytesLeftInBuffer -= 1;
	value = get8bit(source);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, int16_t& value) {
	verifySize(value, bytesLeftInBuffer);
	bytesLeftInBuffer -= 2;
	value = get16bit(source);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, int32_t& value) {
	verifySize(value, bytesLeftInBuffer);
	bytesLeftInBuffer -= 4;
	value = get32bit(source);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, int64_t& value) {
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

// deserialize a string
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, std::string& value) {
	sassert(value.size() == 0);
	uint32_t size;
	deserialize(source, bytesLeftInBuffer, size);
	// size is length of the string + 1 -- the last byte is a null terminator
	if (size > kMaxDeserializedElementsCount) {
		throw IncorrectDeserializationException("untrustworthy string size");
	}
	if (bytesLeftInBuffer < size) {
		throw IncorrectDeserializationException("unexpected end of buffer");
	}
	if ((*source)[size - 1] != 0) {
		throw IncorrectDeserializationException("deserialized string not null-terminated");
	}
	// create a string from the buffer, but without the last (null) character
	value.assign(reinterpret_cast<const char*>(*source), size - 1);
	bytesLeftInBuffer -= size;
	*source += size;
}

// deserialize a unique_ptr
template<class T>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		std::unique_ptr<T>& ptr) {
	sassert(!ptr);
	bool isNotEmpty;
	deserialize(source, bytesLeftInBuffer, isNotEmpty);
	if (isNotEmpty) {
		ptr.reset(new T());
		deserialize(source, bytesLeftInBuffer, *ptr);
	}
}

template<class T>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		std::vector<T>& vec) {
	sassert(vec.size() == 0);
	uint32_t size;
	deserialize(source, bytesLeftInBuffer, size);
	if (size > kMaxDeserializedElementsCount) {
		throw IncorrectDeserializationException("untrustworthy vector size");
	}
	vec.resize(size);
	for (unsigned i = 0; i < size; ++i) {
		deserialize(source, bytesLeftInBuffer, vec[i]);
	}
}

template<class T>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, T& t) {
	return t.deserialize(source, bytesLeftInBuffer);
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
	T dummy;
	deserialize(source, bytesLeftInBuffer, dummy);
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
	if (sourceBufferSize > kMaxDeserializedBytesCount) {
		throw IncorrectDeserializationException("too much data to deserialize");
	}
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
