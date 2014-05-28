#pragma once

#include "config.h"

#include <sys/types.h>
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/io_limit_group.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(GetIoLimitGroupIdException, Exception);

// parse 'is' assuming that it conains /proc/*/cgroup - formatted data
IoLimitGroupId getIoLimitGroupId(std::istream& is, const std::string& subsystem);

// parse /proc/pid/cgroup
IoLimitGroupId getIoLimitGroupId(const pid_t pid, const std::string& subsystem);

// like above, return "unclassified" on error (which is not a valid cgroup name)
IoLimitGroupId getIoLimitGroupIdNoExcept(const pid_t pid, const std::string& subsystem);
