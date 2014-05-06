#include "config.h"
#include "master/quota_database.h"

#include <unordered_map>

// Data structures and helper function used to implement QuotaDatabase
class QuotaDatabaseImplementation {
public:
	std::unordered_map<uint32_t, QuotaLimits> gidData, uidData;

	// Returns limits. If it didn't exist -- creates an empty one
	QuotaLimits& getEntry(QuotaOwnerType ownerType, uint32_t ownerId) {
		auto& map = (ownerType == QuotaOwnerType::kUser ? uidData : gidData);
		return map[ownerId];
	}

	QuotaLimits* getEntryOrNull(QuotaOwnerType ownerType, uint32_t ownerId) {
		auto& map = (ownerType == QuotaOwnerType::kUser ? uidData : gidData);
		auto it = map.find(ownerId);
		if (it == map.end()) {
			return nullptr;
		}
		return &it->second;
	}

	// Returns a reference to the requested QuotaLimits' field
	uint64_t& extractLimit(QuotaLimits& limits, QuotaRigor rigor, QuotaResource resource) {
		if (rigor == QuotaRigor::kSoft && resource == QuotaResource::kInodes) {
			return limits.inodesSoftLimit;
		} else if (rigor == QuotaRigor::kHard && resource == QuotaResource::kInodes) {
			return limits.inodesHardLimit;
		} else if (rigor == QuotaRigor::kSoft && resource == QuotaResource::kSize) {
			return limits.bytesSoftLimit;
		} else if (rigor == QuotaRigor::kHard && resource == QuotaResource::kSize) {
			return limits.bytesHardLimit;
		} else {
			throw Exception("This will never happen");
		}
	}

	// Returns a reference to the requested QuotaLimits' field
	uint64_t& extractUsage(QuotaLimits& limits, QuotaResource resource) {
		if (resource == QuotaResource::kInodes) {
			return limits.inodes;
		} else if (resource == QuotaResource::kSize) {
			return limits.bytes;
		} else {
			throw Exception("This will never happen");
		}
	}

	bool isLimitExceeded(QuotaRigor rigor, QuotaResource resource,
			QuotaOwnerType ownerType, uint32_t ownerId) {
		QuotaLimits* limits = getEntryOrNull(ownerType, ownerId);
		if (limits != nullptr) {
			uint64_t limit = extractLimit(*limits, rigor, resource);
			uint64_t usage = extractUsage(*limits, resource);
			if (limit != 0 && usage > limit) {
				return true;
			}
		}
		return false;
	}
};

// The actual implementation starts here

QuotaDatabase::QuotaDatabase() : impl_(new QuotaDatabaseImplementation()) {}

QuotaDatabase::~QuotaDatabase() {}

void QuotaDatabase::set(QuotaRigor rigor, QuotaResource resource,
		QuotaOwnerType ownerType, uint32_t ownerId, uint64_t value) {
	auto& limits = impl_->getEntry(ownerType, ownerId);
	impl_->extractLimit(limits, rigor, resource) = value;
}

void QuotaDatabase::remove(QuotaRigor rigor, QuotaResource resource,
		QuotaOwnerType ownerType, uint32_t ownerId) {
	set(rigor, resource, ownerType, ownerId, 0);
}

bool QuotaDatabase::isExceeded(QuotaRigor rigor, QuotaResource resource,
		uint32_t uid, uint32_t gid) const {
	return impl_->isLimitExceeded(rigor, resource, QuotaOwnerType::kUser, uid)
			|| impl_->isLimitExceeded(rigor, resource, QuotaOwnerType::kGroup, gid);
}

const QuotaLimits* QuotaDatabase::get(QuotaOwnerType ownerType, uint32_t ownerId) const {
	return impl_->getEntryOrNull(ownerType, ownerId);
}

std::vector<QuotaOwnerAndLimits> QuotaDatabase::getAll() const {
	std::vector<QuotaOwnerAndLimits> ret;
	for (const auto& uidAndLimits : impl_->uidData) {
		QuotaOwner owner(QuotaOwnerType::kUser, uidAndLimits.first);
		ret.emplace_back(owner, uidAndLimits.second);
	}
	for (const auto& gidAndLimits : impl_->gidData) {
		QuotaOwner owner(QuotaOwnerType::kGroup, gidAndLimits.first);
		ret.emplace_back(owner, gidAndLimits.second);
	}
	return ret;
}

void QuotaDatabase::changeUsage(QuotaResource resource, uint32_t uid, uint32_t gid, int64_t delta) {
	impl_->extractUsage(impl_->getEntry(QuotaOwnerType::kUser, uid), resource) += delta;
	impl_->extractUsage(impl_->getEntry(QuotaOwnerType::kGroup, gid), resource) += delta;
}
