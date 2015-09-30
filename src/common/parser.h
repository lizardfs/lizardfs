/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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

#include <cassert>
#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <string>

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
		if(length / 2 > sizeof(T)) {
			throw std::out_of_range("too big hex value passed to parser");
		}
		if (number.at(0) == '-') {
			return T(std::stoll(number.substr(position, length), nullptr, 16));
		} else {
			return T(std::stoull(number.substr(position, length), nullptr, 16));
		}
	}

	template<typename T>
	T intFromDecString(const std::string& number,
			size_t position = 0, size_t length = std::string::npos) {
		if (number.at(0) == '-') {
			return T(std::stoll(number.substr(position, length), nullptr, 10));
		} else {
			return T(std::stoull(number.substr(position, length), nullptr, 10));
		}
	}
};
