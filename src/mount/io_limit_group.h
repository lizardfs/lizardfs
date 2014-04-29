#pragma once

#include "config.h"

#include <sys/types.h>
#include <iostream>
#include <string>

#include "common/exception.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(GetIoLimitGroupIdException, Exception);

std::string getIoLimitGroupId(std::istream& is, const std::string& subsystem);
std::string getIoLimitGroupId(const pid_t pid, const std::string& subsystem);
