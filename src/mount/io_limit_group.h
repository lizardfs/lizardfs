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
