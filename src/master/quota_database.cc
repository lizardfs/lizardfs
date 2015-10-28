/*
   Copyright 2016 Skytechnology sp. z o.o.

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
#include "master/quota_database.h"

void QuotaDatabase::remove(QuotaOwnerType owner_type, uint32_t owner_id, QuotaRigor rigor,
		QuotaResource resource) {
	auto &map = quota_data_[(int)owner_type];
	auto it = map.find(owner_id);
	if (it == map.end()) {
		return;
	}

	it->second[(int)rigor][(int)resource] = 0;
	if (it->second == Limits()) {
		map.erase(it);
	}
}

void QuotaDatabase::remove(QuotaOwnerType owner_type, uint32_t owner_id) {
	auto &map = quota_data_[(int)owner_type];
	auto it = map.find(owner_id);
	if (it == map.end()) {
		return;
	}

	map.erase(it);
}

bool QuotaDatabase::exceeds(QuotaOwnerType owner_type, uint32_t owner_id, QuotaRigor rigor,
		const std::initializer_list<std::pair<QuotaResource, int64_t>> &resource_list) const {
	const Limits *entry = get(owner_type, owner_id);
	if (!entry) {
		return false;
	}

	for (const auto &resource : resource_list) {
		uint64_t limit = (*entry)[(int)rigor][(int)resource.first];
		uint64_t usage = (*entry)[(int)QuotaRigor::kUsed][(int)resource.first] + resource.second;

		if (limit != 0 && usage > limit) {
			return true;
		}
	}

	return false;
}

std::vector<QuotaEntry> QuotaDatabase::getEntries() const {
	std::vector<QuotaEntry> result;

	forEach([&result](QuotaRigor rigor, QuotaResource resource, QuotaOwnerType owner_type,
	                  uint32_t owner_id, const Limits &entry) {
		uint64_t limit = entry[(int)rigor][(int)resource];
		if (limit > 0) {
			result.push_back({{{owner_type, owner_id}, rigor, resource}, limit});
		}
	});

	return result;
}

std::vector<QuotaEntry> QuotaDatabase::getEntriesWithStats() const {
	std::vector<QuotaEntry> result;

	for (auto owner_type :
	     {QuotaOwnerType::kUser, QuotaOwnerType::kGroup, QuotaOwnerType::kInode}) {
		for (const auto &data_entry : quota_data_[(int)owner_type]) {
			for (auto resource : {QuotaResource::kInodes, QuotaResource::kSize}) {
				bool non_zero = false;

				for (auto rigor : {QuotaRigor::kSoft, QuotaRigor::kHard}) {
					uint64_t limit = data_entry.second[(int)rigor][(int)resource];
					if (limit > 0) {
						result.push_back(
						    {{{owner_type, data_entry.first}, rigor, resource}, limit});
						non_zero = true;
					}
				}

				if (non_zero) {
					uint64_t value = data_entry.second[(int)QuotaRigor::kUsed][(int)resource];
					result.push_back(
					    {{{owner_type, data_entry.first}, QuotaRigor::kUsed, resource}, value});
				}
			}
		}
	}

	return result;
}

// Get a checksum of the database (usage doesn't count)
uint64_t QuotaDatabase::checksum() const {
	uint64_t checksum = 0xcd13ca11bcb1beb5;  // some random number

	forEach([this, &checksum](QuotaRigor rigor, QuotaResource resource, QuotaOwnerType owner_type,
	                          uint32_t owner_id, const Limits &entry) {
		uint64_t limit = entry[(int)rigor][(int)resource];
		if (limit > 0) {
			QuotaEntry qentry = {{{owner_type, owner_id}, rigor, resource}, limit};
			addToChecksum(checksum, hash(qentry));
		}
	});

	return checksum;
}
