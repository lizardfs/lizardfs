#pragma once

#include <cstdint>
#include <string>
#include <sstream>

#include "common/massert.h"

class Parser {
public:
	typedef int (*TypeCheckFunction)(int c);

	enum Status {
		OK,
		ERROR_NO_BYTES_LEFT,
		ERROR_NOT_ENOUGH_DATA,
		ERROR_NO_MATCH
	};

	Parser(const std::string& stringToParse);
	virtual ~Parser();
	Status consume(size_t charactersToConsume);
	Status consume(const std::string& stringToConsume);
	Status consume(const TypeCheckFunction& checkType);

protected:
	std::string data() const { return data_; }
	size_t getLastConsumedCharacterCount() const;
	size_t previousPosition() const { return previousPosition_; }
	size_t position() const { return position_; }

	template<typename T>
	T getHexValue() {
		size_t pos = previousPosition();
		size_t length = getLastConsumedCharacterCount();
		return intFromHexString<T>(data(), pos, length);
	}

	template<typename T>
	T getDecValue() {
		size_t pos = previousPosition();
		size_t length = getLastConsumedCharacterCount();
		return intFromDecString<T>(data(), pos, length);
	}

private:
	std::string data_;
	size_t previousPosition_;
	size_t position_;

	Status checkState(int bytesToConsume);

	template<typename T>
	T intFromHexString(const std::string& number,
			size_t position = 0, size_t length = std::string::npos) {
		sassert(length / 2 <= sizeof(T));
		return T(std::stoll(number.substr(position, length), nullptr, 16));
	}

	template<typename T>
	T intFromDecString(const std::string& number,
			size_t position = 0, size_t length = std::string::npos) {
		return T(std::stoll(number.substr(position, length), nullptr, 10));
	}
};
