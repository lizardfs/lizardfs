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

#include "common/platform.h"
#include "common/parser.h"

Parser::Parser(const std::string& stringToParse)
	: data_(stringToParse),
	  previousPosition_(0),
	  position_(0) {
}

Parser::~Parser() {
}

Parser::Status Parser::consume(size_t bytesToConsume) {
	Status status = checkState(bytesToConsume);
	if (status != OK) {
		return status;
	}
	previousPosition_ = position_;
	position_ += bytesToConsume;
	return OK;
}

Parser::Status Parser::consume(const std::string& strToConsume) {
	Status status = checkState(strToConsume.size());
	if (status != OK) {
		return status;
	}
	if (data_.find(strToConsume, position_) != position_) {
		return ERROR_NO_MATCH;
	}
	previousPosition_ = position_;
	position_ += strToConsume.size();
	return OK;
}

Parser::Status Parser::consume(const TypeCheckFunction& checkType) {
	Status status = checkState(0);
	if (status != OK) {
		return status;
	}
	size_t newPosition = position_;
	while (checkType(data_.at(newPosition))) {
		++newPosition;
	}
	if (position_ == newPosition) {
		return ERROR_NO_MATCH;
	}
	previousPosition_ = position_;
	position_ = newPosition;
	return OK;
}

Parser::Status Parser::checkState(int bytesToConsume) {
	if (position_ == data_.size()) {
		return ERROR_NO_BYTES_LEFT;
	} else if (data_.size() < position_ + bytesToConsume) {
		return ERROR_NOT_ENOUGH_DATA;
	} else {
		return OK;
	}
}

size_t Parser::getLastConsumedCharacterCount() const {
	return position_ - previousPosition_;
}
