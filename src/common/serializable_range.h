#pragma once
#include "common/platform.h"

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

	uint32_t serializedSize() const {
		uint32_t ret = ::serializedSize(uint32_t(std::distance(begin(), end())));
		for (const auto& element : *this) {
			ret += ::serializedSize(element);
		}
		return ret;
	}

	void serialize(uint8_t** destination) const {
		uint32_t count = std::distance(begin(), end());
		::serialize(destination, count);
		for (const auto& element : *this) {
			::serialize(destination, element);
		}
	}

private:
	const_iterator begin_;
	const_iterator end_;
};

template <class Range>
inline SerializableRange<typename Range::const_iterator> makeSerializableRange(const Range& range) {
	return SerializableRange<typename Range::const_iterator>(range.begin(), range.end());
}

template <class Iterator>
inline SerializableRange<Iterator> makeSerializableRange(Iterator begin, Iterator end) {
	return SerializableRange<Iterator>(begin, end);
}
