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
#include "common/io_limits_database.h"

void IoLimitsDatabase::setLimits(SteadyTimePoint now,
		const IoLimitsConfigLoader::LimitsMap& limits, uint32_t accumulate_ms) {
	auto limitsIter = limits.begin();
	auto groupsIter = groups_.begin();
	while (true) {
		// remove groups which don't exist anymore
		while (groupsIter != groups_.end() &&
				(limitsIter == limits.end() ||
				 limitsIter->first > groupsIter->first)) {
			groups_.erase(groupsIter++);
		}
		// end of limitsIter => end of work
		if (limitsIter == limits.end()) {
			break;
		}
		// insert new GroupState if a new group appeared
		if (groupsIter == groups_.end() ||
				groupsIter->first > limitsIter->first) {
			groupsIter = groups_.insert(groupsIter,
					std::make_pair(limitsIter->first, TokenBucket(now)));
		}
		// groupsIter->first == limitsIter->first

		// Limit in LimitsMap is in Kbps, below we convert it into Bps:
		groupsIter->second.reconfigure(now,
				limitsIter->second * 1024,
				limitsIter->second * 1024 * accumulate_ms / 1000);
		limitsIter++;
		groupsIter++;
	}
}

std::vector<std::string> IoLimitsDatabase::getGroups() const {
	std::vector<std::string> result;
	for (const auto& group : groups_) {
		result.push_back(group.first);
	}
	return result;
}

std::vector<IoGroupAndLimit> IoLimitsDatabase::getGroupsAndLimits() const {
	std::vector<IoGroupAndLimit> result;
	for (const auto& group : groups_) {
		result.push_back({group.first, (uint64_t)group.second.rate()});
	}
	return result;
}

uint64_t IoLimitsDatabase::request(SteadyTimePoint now, const GroupId& groupId, uint64_t bytes) {
	try {
		return groups_.at(groupId).attempt(now, bytes);
	} catch (std::out_of_range&) {
		throw InvalidGroupIdException();
	}
}
