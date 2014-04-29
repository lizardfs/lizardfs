#include "config.h"
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
