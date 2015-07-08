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
#include "common/io_limits_config_loader.h"

#include <limits>

#include "common/exceptions.h"
#include "common/io_limit_group.h"

inline static bool streamReadFailed(const std::istream& stream) {
	return stream.eof() || stream.fail() || stream.bad();
}

void IoLimitsConfigLoader::load(std::istream&& stream) {
	limits_.clear();

	bool cgroupsInUse = false;
	while (!stream.eof()) {
		std::string command;
		std::string group;
		uint64_t limit;
		stream >> command;
		if (streamReadFailed(stream)) {
			if (stream.eof()) {
				break;
			}
			throw ParseException("Unexpected end of file.");
		}
		if (command == "subsystem") {
			stream >> subsystem_;
			if (streamReadFailed(stream)) {
				throw ParseException("Incorrect file format.");
			}
		} else if (command == "limit") {
			stream >> group >> limit;
			if (streamReadFailed(stream)) {
				throw ParseException("Incorrect file format.");
			} else if (limits_.find(group) != limits_.end()) {
				throw ParseException("Limit for group '" + group +
						"' specified more then once.");
			}
			limits_[group] = limit;
			cgroupsInUse |= (group != kUnclassified);
		} else if (!command.empty() && command.front() == '#') {
			stream.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
		} else {
			throw ParseException("Unknown keyword '" + command + "'.");
		}
	}

	if (cgroupsInUse && subsystem_.empty()) {
		throw ParseException("Subsystem not specified.");
	}
}
