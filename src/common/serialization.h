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

#include <array>
#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/compact_vector.h"
#include "common/flat_set.h"
#include "common/flat_map.h"
#include "common/datapack.h"
#include "common/exception.h"
#include "common/massert.h"
#include "common/small_vector.h"

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

inline uint32_t serializedSize(bool) {
	return 1;
}

inline uint32_t serializedSize(char) {
	return 1;
}

inline uint32_t serializedSize(signed char) {
	return 1;
}

inline uint32_t serializedSize(unsigned char) {
	return 1;
}

inline uint32_t serializedSize(uint16_t) {
	return 2;
}

inline uint32_t serializedSize(uint32_t) {
	return 4;
}

inline uint32_t serializedSize(uint64_t) {
	return 8;
}

inline uint32_t serializedSize(int16_t) {
	return 2;
}

inline uint32_t serializedSize(int32_t) {
	return 4;
}

inline uint32_t serializedSize(int64_t) {
	return 8;
}

inline uint32_t serializedSize(const std::string& value) {
	return serializedSize(uint32_t(value.size())) + value.size() + 1;
}

template <class T, int N>
inline uint32_t serializedSize(const T (&array)[N]);
template <class T, std::size_t N>
inline uint32_t serializedSize(const std::array<T, N>& array);
template<class T>
inline uint32_t serializedSize(const std::unique_ptr<T>& ptr);
template<class T, class A>
inline uint32_t serializedSize(const std::vector<T, A>& vector);
template<class T, size_t Size>
inline uint32_t serializedSize(const small_vector<T, Size> &vector);
template <typename K, typename C, typename A>
inline uint32_t serializedSize(const std::set<K, C, A>& set);
template<class T1, class T2>
inline uint32_t serializedSize(const std::pair<T1, T2>& pair);
template <typename K, typename T, typename C, typename A>
inline uint32_t serializedSize(const std::map<K, T, C, A>& map);
template<class T>
inline uint32_t serializedSize(const T& t);
template<class T, class ... Args>
inline uint32_t serializedSize(const T& t, const Args& ... args);

template <class T, int N>
inline uint32_t serializedSize(const T (&array)[N]) {
	uint32_t ret = 0;
	for (const auto& element : array) {
		ret += serializedSize(element);
	}
	return ret;
}

template <class T, std::size_t N>
inline uint32_t serializedSize(const std::array<T, N>& array) {
	uint32_t ret = 0;
	for (const auto& element : array) {
		ret += serializedSize(element);
	}
	return ret;
}

template<class T1, class T2>
inline uint32_t serializedSize(const std::pair<T1, T2>& pair) {
	return serializedSize(pair.first) + serializedSize(pair.second);
}

template<class T>
inline uint32_t serializedSize(const std::unique_ptr<T>& ptr) {
	if (ptr) {
		return serializedSize(true) + serializedSize(*ptr);
	} else {
		return serializedSize(false);
	}
}

template<class T, class A>
inline uint32_t serializedSize(const std::vector<T, A>& vector) {
	uint32_t ret = 0;
	ret += serializedSize(uint32_t(vector.size()));
	for (const auto& t : vector) {
		ret += serializedSize(t);
	}
	return ret;
}

template<class T, size_t Size>
inline uint32_t serializedSize(const small_vector<T, Size> &vector) {
	uint32_t ret = 0;
	ret += serializedSize(uint32_t(vector.size()));
	for (const auto& t : vector) {
		ret += serializedSize(t);
	}
	return ret;
}

template <typename K, typename C, typename A>
inline uint32_t serializedSize(const std::set<K, C, A>& set) {
	uint32_t ret = 0;
	ret += serializedSize(uint32_t(set.size()));
	for (const auto& t : set) {
		ret += serializedSize(t);
	}
	return ret;
}

template <typename K, typename T, typename C, typename A>
inline uint32_t serializedSize(const std::map<K, T, C, A>& map) {
	uint32_t ret = 0;
	ret += serializedSize(uint32_t(map.size()));
	for (const auto& t : map) {
		ret += serializedSize(t);
	}
	return ret;
}

template <typename T, typename Size, typename Alloc>
inline uint32_t serializedSize(const compact_vector<T, Size, Alloc>& vector) {
	uint32_t ret = 0;
	ret += serializedSize(uint32_t(vector.size()));
	for (const auto& t : vector) {
		ret += serializedSize(t);
	}
	return ret;
}

template <typename T, typename C, class Compare>
inline uint32_t serializedSize(const flat_set<T, C, Compare>& set) {
	return serializedSize(set.data());
}

template <typename Key, typename T, typename C, class Compare>
inline uint32_t serializedSize(const flat_map<Key, T, C, Compare>& map) {
	return serializedSize(map.data());
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
inline void serialize(uint8_t** destination, bool value) {
	put8bit(destination, static_cast<uint8_t>(value ? 1 : 0));
}

inline void serialize(uint8_t** destination, char value) {
	put8bit(destination, value);
}

inline void serialize(uint8_t** destination, signed char value) {
	put8bit(destination, value);
}

inline void serialize(uint8_t** destination, unsigned char value) {
	put8bit(destination, value);
}

inline void serialize(uint8_t** destination, uint16_t value) {
	put16bit(destination, value);
}

inline void serialize(uint8_t** destination, uint32_t value) {
	put32bit(destination, value);
}

inline void serialize(uint8_t** destination, uint64_t value) {
	put64bit(destination, value);
}

inline void serialize(uint8_t** destination, int16_t value) {
	put16bit(destination, value);
}

inline void serialize(uint8_t** destination, int32_t value) {
	put32bit(destination, value);
}

inline void serialize(uint8_t** destination, int64_t value) {
	put64bit(destination, value);
}

// serialize a string
inline void serialize(uint8_t** destination, const std::string& value) {
	serialize(destination, uint32_t(value.length() + 1));
	memcpy(*destination, value.data(), value.length());
	*destination += value.length();
	serialize(destination, char(0));
}

template <class T, int N>
inline void serialize(uint8_t** destination, const T (&array)[N]);
template <class T, std::size_t N>
inline void serialize(uint8_t** destination, const std::array<T, N>& array);
template<class T1, class T2>
inline void serialize(uint8_t** destination, const std::pair<T1, T2>& pair);
template<class T>
inline void serialize(uint8_t** destination, const std::unique_ptr<T>& ptr);
template<class T, class A>
inline void serialize(uint8_t** destination, const std::vector<T, A>& vector);
template<class T, size_t Size>
inline void serialize(uint8_t **destination, const small_vector<T, Size> &vector);
template <typename K, typename C, typename A>
inline void serialize(uint8_t** destination, const std::set<K, C, A>& set);
template <typename K, typename T, typename C, typename A>
inline void serialize(uint8_t** destination, const std::map<K, T, C, A>& map);
template<class T>
inline void serialize(uint8_t** destination, const T& t);
template<class T, class... Args>
inline void serialize(uint8_t** destination, const T& t, const Args&... args);

// serialize fixed size array ("type name[number];")
template <class T, int N>
inline void serialize(uint8_t** destination, const T (&array)[N]) {
	for (int i = 0; i < N; i++) {
		serialize(destination, array[i]);
	}
}

template <class T, std::size_t N>
inline void serialize(uint8_t** destination, const std::array<T, N>& array) {
	for (std::size_t i = 0; i < N; i++) {
		serialize(destination, array[i]);
	}
}

// serialize a pair
template<class T1, class T2>
inline void serialize(uint8_t** destination, const std::pair<T1, T2>& pair) {
	serialize(destination, pair.first);
	serialize(destination, pair.second);
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
template<class T, class A>
inline void serialize(uint8_t** destination, const std::vector<T, A>& vector) {
	serialize(destination, uint32_t(vector.size()));
	for (const T& t : vector) {
		serialize(destination, t);
	}
}

template<class T, size_t Size>
inline void serialize(uint8_t **destination, const small_vector<T, Size> &vector) {
	serialize(destination, uint32_t(vector.size()));
	for (const T& t : vector) {
		serialize(destination, t);
	}
}

template <typename K, typename C, typename A>
inline void serialize(uint8_t** destination, const std::set<K, C, A>& set) {
	serialize(destination, uint32_t(set.size()));
	for (const auto& t : set) {
		serialize(destination, t);
	}
}

template <typename K, typename T, typename C, typename A>
inline void serialize(uint8_t** destination, const std::map<K, T, C, A>& map) {
	serialize(destination, uint32_t(map.size()));
	for (const auto& t : map) {
		serialize(destination, t);
	}
}

template <typename T, typename Size, typename Alloc>
inline void serialize(uint8_t** destination, const compact_vector<T, Size, Alloc>& vector) {
	serialize(destination, uint32_t(vector.size()));
	for (const T& t : vector) {
		serialize(destination, t);
	}
}

template <typename T, typename C, class Compare>
inline void serialize(uint8_t** destination, const flat_set<T, C, Compare>& set) {
	serialize(destination, set.data());
}
template <typename Key, typename T, typename C, class Compare>
inline void serialize(uint8_t** destination, const flat_map<Key, T, C, Compare>& map) {
	serialize(destination, map.data());
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

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, char& value) {
	verifySize(value, bytesLeftInBuffer);
	bytesLeftInBuffer -= 1;
	value = get8bit(source);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, signed char& value) {
	verifySize(value, bytesLeftInBuffer);
	bytesLeftInBuffer -= 1;
	value = get8bit(source);
}


inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, unsigned char& value) {
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

template <class T, int N>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, T (&array)[N]);
template <class T, std::size_t N>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		std::array<T, N>& array);
template<class T1, class T2>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		std::pair<T1, T2>& pair);
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, const uint8_t*& value);
template<class T>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		std::unique_ptr<T>& ptr);
template<class T, class A>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		std::vector<T, A>& vec);
template<class T, size_t Size>
inline void deserialize(const uint8_t **source, uint32_t& bytesLeftInBuffer,
		small_vector<T, Size> &vec);
template <typename K, typename C, typename A>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		std::set<K, C, A>& set);
template <typename K, typename T, typename C, typename A>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		std::map<K, T, C, A>& map);
template<class T>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, T& t);
template<class T, class... Args>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, T& t, Args&... args);

// deserialize fixed size array ("type name[number];")
template <class T, int N>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, T (&array)[N]) {
	for (int i = 0; i < N; i++) {
		deserialize(source, bytesLeftInBuffer, array[i]);
	}
}

template <class T, std::size_t N>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		std::array<T, N>& array) {
	for (std::size_t i = 0; i < N; i++) {
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

template<class T, class A>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		std::vector<T, A>& vec) {
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

template<class T, size_t Size>
inline void deserialize(const uint8_t **source, uint32_t& bytesLeftInBuffer,
		small_vector<T, Size> &vec) {
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

template <typename K, typename C, typename A>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		std::set<K, C, A>& set) {
	sassert(set.size() == 0);
	uint32_t size;
	deserialize(source, bytesLeftInBuffer, size);
	if (size > kMaxDeserializedElementsCount) {
		throw IncorrectDeserializationException("untrustworthy set size");
	}
	for (unsigned i = 0; i < size; ++i) {
		K element;
		deserialize(source, bytesLeftInBuffer, element);
		set.insert(std::move(element));
	}
}

template <typename K, typename T, typename C, typename A>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		std::map<K, T, C, A>& map) {
	sassert(map.size() == 0);
	uint32_t size;
	deserialize(source, bytesLeftInBuffer, size);
	if (size > kMaxDeserializedElementsCount) {
		throw IncorrectDeserializationException("untrustworthy map size");
	}
	for (unsigned i = 0; i < size; ++i) {
		std::pair<K, T> v;
		deserialize(source, bytesLeftInBuffer, v);
		map.insert(std::move(v));
	}
}

template<typename T, typename Size, typename Alloc>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		compact_vector<T, Size, Alloc>& vec) {
	sassert(vec.size() == 0);
	uint32_t size;
	deserialize(source, bytesLeftInBuffer, size);
	if (size > kMaxDeserializedElementsCount) {
		throw IncorrectDeserializationException("untrustworthy compact_vector size");
	}
	vec.resize(size);
	for(auto &element : vec) {
		deserialize(source, bytesLeftInBuffer, element);
	}
}

template <typename T, typename C, class Compare>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		flat_set<T, C, Compare>& set) {
	deserialize(source, bytesLeftInBuffer, set.data());
}

template <typename Key, typename T, typename C, class Compare>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		flat_map<Key, T, C, Compare>& map) {
	deserialize(source, bytesLeftInBuffer, map.data());
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
