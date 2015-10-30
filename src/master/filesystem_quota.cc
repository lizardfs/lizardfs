/*
   Copyright 2013-2015 Skytechnology sp. z o.o..

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"

#include <cassert>

#include "common/main.h"
#include "master/filesystem_checksum_updater.h"
#include "master/filesystem_metadata.h"
#include "master/quota_database.h"

template <class T>
bool decodeChar(const char *keys, const std::vector<T> values, char key, T &value) {
	const uint32_t count = strlen(keys);
	sassert(values.size() == count);
	for (uint32_t i = 0; i < count; i++) {
		if (key == keys[i]) {
			value = values[i];
			return true;
		}
	}
	return false;
}

#ifndef METARESTORE
uint8_t fs_quota_get_all(uint8_t sesflags, uint32_t uid,
			std::vector<QuotaOwnerAndLimits> &results) {
	if (uid != 0 && !(sesflags & SESFLAG_ALLCANCHANGEQUOTA)) {
		return LIZARDFS_ERROR_EPERM;
	}
	results = gMetadata->quota_database.getAll();
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_quota_get(uint8_t sesflags, uint32_t uid, uint32_t gid,
		const std::vector<QuotaOwner> &owners,
		std::vector<QuotaOwnerAndLimits> &results) {
	std::vector<QuotaOwnerAndLimits> tmp;
	for (const QuotaOwner &owner : owners) {
		if (uid != 0 && !(sesflags & SESFLAG_ALLCANCHANGEQUOTA)) {
			switch (owner.ownerType) {
			case QuotaOwnerType::kUser:
				if (uid != owner.ownerId) {
					return LIZARDFS_ERROR_EPERM;
				}
				break;
			case QuotaOwnerType::kGroup:
				if (gid != owner.ownerId && !(sesflags & SESFLAG_IGNOREGID)) {
					return LIZARDFS_ERROR_EPERM;
				}
				break;
			default:
				return LIZARDFS_ERROR_EINVAL;
			}
		}
		const QuotaLimits *result =
		        gMetadata->quota_database.get(owner.ownerType, owner.ownerId);
		if (result) {
			tmp.emplace_back(owner, *result);
		}
	}
	results.swap(tmp);
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_quota_set(uint8_t sesflags, uint32_t uid, const std::vector<QuotaEntry> &entries) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	if (sesflags & SESFLAG_READONLY) {
		return LIZARDFS_ERROR_EROFS;
	}
	if (uid != 0 && !(sesflags & SESFLAG_ALLCANCHANGEQUOTA)) {
		return LIZARDFS_ERROR_EPERM;
	}
	for (const QuotaEntry &entry : entries) {
		const QuotaOwner &owner = entry.entryKey.owner;
		gMetadata->quota_database.set(entry.entryKey.rigor, entry.entryKey.resource,
		                              owner.ownerType, owner.ownerId, entry.limit);
		fs_changelog(ts, "SETQUOTA(%c,%c,%c,%" PRIu32 ",%" PRIu64 ")",
		             (entry.entryKey.rigor == QuotaRigor::kSoft) ? 'S' : 'H',
		             (entry.entryKey.resource == QuotaResource::kSize) ? 'S' : 'I',
		             (owner.ownerType == QuotaOwnerType::kUser) ? 'U' : 'G',
		             uint32_t{owner.ownerId}, uint64_t{entry.limit});
	}
	return LIZARDFS_STATUS_OK;
}
#endif

uint8_t fs_apply_setquota(char rigor, char resource, char ownerType, uint32_t ownerId,
			uint64_t limit) {
	QuotaRigor quotaRigor = QuotaRigor::kSoft;
	QuotaResource quotaResource = QuotaResource::kSize;
	QuotaOwnerType quotaOwnerType = QuotaOwnerType::kUser;
	bool valid = true;
	valid &= decodeChar("SH", {QuotaRigor::kSoft, QuotaRigor::kHard}, rigor, quotaRigor);
	valid &= decodeChar("SI", {QuotaResource::kSize, QuotaResource::kInodes}, resource,
	                    quotaResource);
	valid &= decodeChar("UG", {QuotaOwnerType::kUser, QuotaOwnerType::kGroup}, ownerType,
	                    quotaOwnerType);
	if (!valid) {
		return LIZARDFS_ERROR_EINVAL;
	}
	gMetadata->metaversion++;
	gMetadata->quota_database.set(quotaRigor, quotaResource, quotaOwnerType, ownerId, limit);
	return LIZARDFS_STATUS_OK;
}

bool fsnodes_inode_quota_exceeded(uint32_t uid, uint32_t gid) {
	return gMetadata->quota_database.isExceeded(QuotaRigor::kHard, QuotaResource::kInodes, uid,
	                                            gid);
}

bool fsnodes_size_quota_exceeded(uint32_t uid, uint32_t gid) {
	return gMetadata->quota_database.isExceeded(QuotaRigor::kHard, QuotaResource::kSize, uid,
	                                            gid);
}

void fsnodes_quota_register_inode(fsnode *node) {
	gMetadata->quota_database.changeUsage(QuotaResource::kInodes, node->uid, node->gid, +1);
}

void fsnodes_quota_unregister_inode(fsnode *node) {
	gMetadata->quota_database.changeUsage(QuotaResource::kInodes, node->uid, node->gid, -1);
}

void fsnodes_quota_update_size(fsnode *node, int64_t delta) {
	if (delta != 0) {
		gMetadata->quota_database.changeUsage(QuotaResource::kSize, node->uid, node->gid,
		                                      delta);
	}
}
