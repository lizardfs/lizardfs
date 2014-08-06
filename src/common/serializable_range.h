#pragma once

#include <cstdint>
#include <iterator>

#include "common/serialization.h"

template <typename Iterator>
class SerializableRange {
public:
	typedef const Iterator const_iterator;
	typedef typename Iterator::value_type value_type;

	SerializableRange(const_iterator begin, const_iterator end) : begin_(begin), end_(end) {}
	const_iterator begin() const { return begin_; }
	const_iterator end() const { return end_; }

private:
	const_iterator begin_;
	const_iterator end_;
};

template<class T>
inline uint32_t serializedSize(const SerializableRange<T>& range) {
	uint32_t count = std::distance(range.begin(), range.end());
	if (count == 0) {
		return 0;
	}
	uint32_t size = serializedSize(*range.begin());
	return count * size;
}

template <typename T>
inline void serialize(uint8_t** destination, const SerializableRange<T>& range) {
	for (const auto& element : range) {
		serialize(destination, element);
	}
}

template <class Range>
inline SerializableRange<typename Range::const_iterator> makeSerializableRange(const Range& range) {
	return SerializableRange<typename Range::const_iterator>(range.begin(), range.end());
}

template <class Iterator>
inline SerializableRange<Iterator> makeSerializableRange(Iterator begin, Iterator end) {
	return SerializableRange<Iterator>(begin, end);
}
