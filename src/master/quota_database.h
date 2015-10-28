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

#pragma once

#include "common/platform.h"

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "common/hashfn.h"
#include "protocol/quota.h"

class QuotaDatabase {
public:
	typedef std::array<std::array<uint64_t, 2>, 3> Limits;
	typedef std::unordered_map<uint32_t, Limits> DataTable;

public:
	QuotaDatabase() = default;


	/*! \brief Gets information about the given owner's usage of resources and limits.
	 * \param owner_type Quota entry type (user, group, inode (directory)).
	 * \param owner_id Entry id.
	 * \return Pointer to full set of limits.
	 */
	const Limits *get(QuotaOwnerType owner_type, uint32_t owner_id) const {
		const auto &map = quota_data_[(int)owner_type];
		auto it = map.find(owner_id);
		if (it == map.end()) {
			return nullptr;
		}

		return &(it->second);
	}

	/*! \brief Set quota for specific resource.
	 * \param owner_type Quota entry type (user, group, inode (directory)).
	 * \param owner_id Entry id.
	 * \param rigor Resource rigor (soft, hard, used).
	 * \param resource Resource type (number of inodes, size of file).
	 * \param value Resource value to set.
	 */
	void set(QuotaOwnerType owner_type, uint32_t owner_id, QuotaRigor rigor, QuotaResource resource,
	         uint64_t value) {
		quota_data_[(int)owner_type][(int)owner_id][(int)rigor][(int)resource] = value;
	}

	/*! \brief Update quota for specific resource.
	 * \param owner_type Quota entry type (user, group, inode (directory)).
	 * \param owner_id Entry id.
	 * \param rigor Resource rigor (soft, hard, used).
	 * \param resource Resource type (number of inodes, size of file).
	 * \param delta New resource value is equal to old value plus \param delta.
	 */
	void update(QuotaOwnerType owner_type, uint32_t owner_id, QuotaRigor rigor,
	            QuotaResource resource, int64_t delta) {
		quota_data_[(int)owner_type][(int)owner_id][(int)rigor][(int)resource] += delta;
	}

	/*! \brief Remove quota for specific resource.
	 * \param owner_type Quota entry type (user, group, inode (directory)).
	 * \param owner_id Entry id.
	 * \param rigor Resource rigor (soft, hard, used).
	 * \param resource Resource type (number of inodes, size of file).
	 */
	void remove(QuotaOwnerType owner_type, uint32_t owner_id, QuotaRigor rigor,
	            QuotaResource resource);

	/*! \brief Remove all resource quotas for specific entry.
	 * \param owner_type Quota entry type (user, group, inode (directory)).
	 * \param owner_id Entry id.
	 */
	void remove(QuotaOwnerType owner_type, uint32_t owner_id);

	/*! \brief Remove cleared resources for specific entry.
	 * \param owner_type Quota entry type (user, group, inode (directory)).
	 * \param owner_id Entry id.
	 */
	void removeEmpty(QuotaOwnerType owner_type, uint32_t owner_id) {
		auto &map = quota_data_[(int)owner_type];
		auto it = map.find(owner_id);
		if (it != map.end()) {
			if (it->second == Limits{{{{0}}}}) {  // workaround for a bug in gcc 4.6
				map.erase(it);
			}
		}
	}

	/*! \brief Checks if changing resource exceeds quota.
	 * \param owner_type Quota entry type (user, group, inode (directory)).
	 * \param owner_id Entry id.
	 * \param rigor Resource rigor (soft, hard, used).
	 * \param resource_list List of resources to check (with change value).
	 */
	bool exceeds(
	    QuotaOwnerType owner_type, uint32_t owner_id, QuotaRigor rigor,
	    const std::initializer_list<std::pair<QuotaResource, int64_t>> &resource_list) const;

	/*! \brief Returns all quota entries (with used). */
	std::vector<QuotaEntry> getEntriesWithStats() const;

	/*! \brief Returns all quota entries (without used). */
	std::vector<QuotaEntry> getEntries() const;

	/*! Get a checksum of the database (usage is not included). */
	uint64_t checksum() const;

protected:
	static uint64_t hash(const QuotaEntry &entry) {
		uint64_t hash = 0x2a9ae768d80f202f;  // some random number
		hashCombine(hash, static_cast<uint8_t>(entry.entryKey.owner.ownerType),
		            static_cast<uint8_t>(entry.entryKey.owner.ownerId),
		            static_cast<uint8_t>(entry.entryKey.rigor),
		            static_cast<uint8_t>(entry.entryKey.resource), entry.limit);
		return hash;
	}

	template<typename Func>
	void forEach(Func func) const {
		for(auto owner_type : {QuotaOwnerType::kUser, QuotaOwnerType::kGroup, QuotaOwnerType::kInode}) {
			for (const auto &data_entry : quota_data_[(int)owner_type]) {
				for (auto rigor : {QuotaRigor::kSoft, QuotaRigor::kHard}) {
					for (auto resource : {QuotaResource::kInodes, QuotaResource::kSize}) {
						func(rigor, resource, owner_type, data_entry.first, data_entry.second);
					}
				}
			}
		}
	}

	std::array<DataTable, 3> quota_data_;
};
