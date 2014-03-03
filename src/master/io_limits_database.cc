#include "master/io_limits_database.h"

void IoLimitsDatabase::setLimits(const IoLimitsConfigLoader::LimitsMap& limits) {
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
					std::make_pair(limitsIter->first, GroupState()));
		}
		// groupsIter->first == limitsIter->first
		groupsIter->second.limit = limitsIter->second;
		limitsIter++;
		groupsIter++;
	}
}

void IoLimitsDatabase::addClient(ClientId id) {
	std::pair<Clients::iterator, bool> result = clients_.insert(id);
	if (!result.second) {
		// this client already exists
		throw ClientExistsException();
	}
}

void IoLimitsDatabase::removeClient(ClientId id) {
	size_t result = clients_.erase(id);
	if (result == 0) {
		// no such client existed
		throw InvalidClientIdException();
	}
	// remove client's allocations in all groups
	for (auto& groupIter : groups_) {
		GroupState& group = groupIter.second;
		auto allocationIter = group.allocations.find(id);
		if (allocationIter != group.allocations.end()) {
			group.allocated -= allocationIter->second;
			group.allocations.erase(allocationIter);
		}
	}
}

uint32_t IoLimitsDatabase::setAllocation(ClientId id, const GroupId& groupId, const uint32_t goal) {
	GroupState& group = getGroup(groupId);
	Allocation& allocation = getAllocation(group, id);
	int64_t amount = 0;
	if (goal > allocation && group.allocated < group.limit) {
		// grow
		const uint32_t available = group.limit - group.allocated;
		const uint32_t requested = goal - allocation;
		amount = std::min(available, requested);
	} else {
		// shrink
		const uint32_t overallocated = (group.allocated > group.limit) ?
				group.allocated - group.limit : 0;
		const uint32_t freed = (allocation > goal) ?
				allocation - goal : 0;
		amount = -int64_t(std::min(allocation, std::max(overallocated, freed)));
	}
	allocation += amount;
	group.allocated += amount;
	return allocation;
}

uint32_t IoLimitsDatabase::getAllocation(ClientId id, const GroupId& groupId) {
	GroupState& group = getGroup(groupId);
	Allocation& allocation = getAllocation(group, id);
	return allocation;
}

IoLimitsDatabase::GroupState& IoLimitsDatabase::getGroup(const GroupId& id) {
	auto group = groups_.find(id);
	if (group == groups_.end()) {
		throw InvalidGroupIdException();
	}
	return group->second;
}

IoLimitsDatabase::Allocation& IoLimitsDatabase::getAllocation(GroupState& group, ClientId id) {
	auto alloc = group.allocations.find(id);
	if (alloc == group.allocations.end()) {
		// this client never allocated bandwidth in this group
		if (clients_.count(id) > 0) {
			// valid client, create an entry for it
			alloc = group.allocations.insert(alloc, {id, Allocation()});
		} else {
			// no such client
			throw InvalidClientIdException();
		}
	}
	return alloc->second;
}
