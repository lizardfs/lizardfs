#pragma once

#include "config.h"

#include <cstdint>
#include <memory>

#include "common/quota.h"

class QuotaDatabaseImplementation;

class QuotaDatabase {
public:
	QuotaDatabase();
	~QuotaDatabase();

	// Sets the given limit
	void set(QuotaRigor rigor, QuotaResource resource,
			QuotaOwnerType ownerType, uint32_t ownerId, uint64_t value);

	// Removes the given limit
	void remove(QuotaRigor rigor, QuotaResource resource,
			QuotaOwnerType ownerType, uint32_t ownerId);

	// Checks if uid/gid pair didn't exceed the given limit
	bool isExceeded(QuotaRigor rigor, QuotaResource resource,
			uint32_t uid, uint32_t gid) const;

	// Gets information about the given owner's usage of resources and limits
	const QuotaLimits* get(QuotaOwnerType ownerType, uint32_t ownerId) const;

	// Returns all quotas and usages
	std::vector<QuotaOwnerAndLimits> getAll() const;

	// Increases/decreases usage of the given resource by the given owner
	void changeUsage(QuotaResource resource, uint32_t uid, uint32_t gid, int64_t delta);

private:
	std::unique_ptr<QuotaDatabaseImplementation> impl_;
};
