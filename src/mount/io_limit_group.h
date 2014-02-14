#pragma once

#include <sys/types.h>

#include <iostream>
#include <string>

#include "common/exception.h"

class GetIoLimitGroupIdException : public Exception {
public:
	GetIoLimitGroupIdException(const std::string& msg) : Exception(msg) {
	}
};

std::string getIoLimitGroupId(std::istream& is, const std::string& subsystem);
std::string getIoLimitGroupId(const pid_t pid, const std::string subsystem);
