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

#include "common/platform.h"
#include "mount/io_limit_group.h"

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

static void skipHierarchy(std::istream& is) {
	char x;
	do {
		is >> x;
	} while (x != ':');
}

static bool searchSubsystems(std::istream& is, const std::string& subsystem) {
	const int length = subsystem.length();
	int matched = 0;
	char x;
	while (1) {
		is >> x;
		if (matched == length && (x == ',' || x == ':')) {
			while (x != ':') {
				is >> x;
			}
			return true;
		}
		if (matched == length || x != subsystem[matched]) {
			while (x != ',' && x != ':') {
				is >> x;
			}
			if (x == ':') {
				return false;
			}
			matched = 0;
			continue;
		}
		matched++;
	}
}

IoLimitGroupId getIoLimitGroupId(std::istream& input, const std::string& subsystem) {
	try {
		for (std::string line; std::getline(input, line);) {
			try {
				std::stringstream ss(line);
				ss.exceptions(std::stringstream::eofbit);
				skipHierarchy(ss);
				if (searchSubsystems(ss, subsystem)) {
					ss.exceptions(std::stringstream::goodbit);
					std::string groupId;
					std::getline(ss, groupId);
					return groupId;
				}
			} catch (std::ios_base::failure&) {
				throw GetIoLimitGroupIdException("Parse error");
			}
		}
	} catch (std::exception&) {
		// Under clang std::getline can throw other exception
		// than std::ios_base::failure when encountering eof.
		if (!input.eof()) {
			throw;
		}
	}

	throw GetIoLimitGroupIdException("Can't find subsystem '" + subsystem + "'");
}

IoLimitGroupId getIoLimitGroupId(const pid_t pid, const std::string& subsystem) {
	char filename[32];
	sprintf(filename, "/proc/%u/cgroup", (unsigned)pid);
	try {
		std::ifstream ifs;
		ifs.exceptions(std::ifstream::failbit | std::ifstream::badbit);
		ifs.open(filename);
		return getIoLimitGroupId(ifs, subsystem);
	} catch (std::ios_base::failure& ex) {
		throw GetIoLimitGroupIdException(
				"Error reading '" + std::string(filename) + ": " + ex.what());
	}
}

IoLimitGroupId getIoLimitGroupIdNoExcept(const pid_t pid, const std::string& subsystem) {
	try {
		return getIoLimitGroupId(pid, subsystem);
	} catch (GetIoLimitGroupIdException&) {
		return kUnclassified;
	}
}
