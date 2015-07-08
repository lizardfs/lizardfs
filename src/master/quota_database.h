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

#include <cstdint>
#include <memory>

#include "common/quota.h"

class QuotaDatabaseImplementation;

class QuotaDatabase {
public:
	QuotaDatabase();
	~QuotaDatabase();

	// Sets the given limit
	void set(QuotaRigor rigor, QuotaResource resource, QuotaOwnerType ownerType, uint32_t ownerId,
			uint64_t value);

	// Removes the given limit
	void remove(QuotaRigor rigor, QuotaResource resource,
			QuotaOwnerType ownerType, uint32_t ownerId);

	// Checks if uid/gid pair didn't exceed the given limit
	bool isExceeded(QuotaRigor rigor, QuotaResource resource, uint32_t uid, uint32_t gid) const;

	// Gets information about the given owner's usage of resources and limits
	const QuotaLimits* get(QuotaOwnerType ownerType, uint32_t ownerId) const;

	// Returns all quotas and usages
	std::vector<QuotaOwnerAndLimits> getAll() const;

	// Returns all quota entries
	std::vector<QuotaEntry> getEntries() const;

	// Increases/decreases usage of the given resource by the given owner
	void changeUsage(QuotaResource resource, uint32_t uid, uint32_t gid, int64_t delta);

	// Get a checksum of the database (usage doesn't count)
	uint64_t checksum() const;

private:
	std::unique_ptr<QuotaDatabaseImplementation> impl_;
};
