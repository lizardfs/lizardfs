/*
   Copyright 2013-2016 Skytechnology sp. z o.o..

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

#include "common/small_vector.h"
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
/*! \brief Remove entries that are not descendants of \param root_inode. */
static void fs_remove_invisible_quota_entries(uint32_t root_inode, std::vector<QuotaEntry> &results) {
	if (root_inode == SPECIAL_INODE_ROOT) {
		return;
	}

	fsnode *root_node = fsnodes_id_to_node(root_inode);
	assert(root_inode);

	auto it = std::remove_if(results.begin(), results.end(), [root_node](const QuotaEntry &entry) {
		if (entry.entryKey.owner.ownerType == QuotaOwnerType::kInode) {
			fsnode *node = fsnodes_id_to_node(entry.entryKey.owner.ownerId);
			if (!node) {
				return true;
			}
			return !fsnodes_isancestor(root_node, node);
		}
		return false;
	});
	results.erase(it, results.end());
}

uint8_t fs_quota_get_all(uint8_t sesflags, uint32_t root_inode, uint32_t uid,
		std::vector<QuotaEntry> &results) {
	if (uid != 0 && !(sesflags & SESFLAG_ALLCANCHANGEQUOTA)) {
		return LIZARDFS_ERROR_EPERM;
	}
	results = gMetadata->quota_database.getEntriesWithStats();

	for (auto &entry : results) {
		if (entry.entryKey.owner.ownerType != QuotaOwnerType::kInode ||
		    entry.entryKey.rigor != QuotaRigor::kUsed) {
			continue;
		}

		fsnode *node = fsnodes_id_to_node(entry.entryKey.owner.ownerId);
		if (!node || node->type != TYPE_DIRECTORY) {
			continue;
		}

		switch (entry.entryKey.resource) {
		case QuotaResource::kSize:
			entry.limit = node->data.ddata.stats->size;
			break;
		case QuotaResource::kInodes:
			entry.limit = node->data.ddata.stats->inodes;
			break;
		}
	}

	fs_remove_invisible_quota_entries(root_inode, results);

	return LIZARDFS_STATUS_OK;
}

uint8_t fs_quota_get(uint8_t sesflags, uint32_t root_inode, uint32_t uid, uint32_t gid,
		const std::vector<QuotaOwner> &owners, std::vector<QuotaEntry> &results) {
	std::vector<QuotaEntry> tmp;
	fsnode *node;
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
			case QuotaOwnerType::kInode:
				node = fsnodes_id_to_node(owner.ownerId);
				if (!node || node->type != TYPE_DIRECTORY) {
					return LIZARDFS_ERROR_EINVAL;
				}
				if (node->uid != uid || (node->gid != gid && !(sesflags & SESFLAG_IGNOREGID))) {
					return LIZARDFS_ERROR_EPERM;
				}
				break;
			default:
				return LIZARDFS_ERROR_EINVAL;
			}
		}
		auto result = gMetadata->quota_database.get(owner.ownerType, owner.ownerId);
		if (result) {
			for (auto rigor : {QuotaRigor::kSoft, QuotaRigor::kHard, QuotaRigor::kUsed}) {
				if (owner.ownerType == QuotaOwnerType::kInode && rigor == QuotaRigor::kUsed) {
					node = fsnodes_id_to_node(owner.ownerId);
					assert(node);
					tmp.push_back({{owner, rigor, QuotaResource::kInodes},
					               (uint64_t)node->data.ddata.stats->inodes});
					tmp.push_back({{owner, rigor, QuotaResource::kSize},
					               (uint64_t)node->data.ddata.stats->size});
					continue;
				}
				for (auto resource : {QuotaResource::kInodes, QuotaResource::kSize}) {
					tmp.push_back({{owner, rigor, resource}, (*result)[(int)rigor][(int)resource]});
				}
			}
		}
	}

	fs_remove_invisible_quota_entries(root_inode, tmp);
	results = std::move(tmp);

	return LIZARDFS_STATUS_OK;
}

static void fsnodes_getpath(uint32_t root_inode, fsnode *node, std::string &ret) {
	std::string::size_type size;
	fsnode *p;

	if (node->id == root_inode) {
		ret = "/";
		return;
	}

	p = node;
	size = 0;
	while (p != gMetadata->root && p->parents && p->id != root_inode) {
		size += p->parents->nleng + 1;  // get first parent !!!
		p = p->parents->parent;  // when folders can be hardlinked it's the only way to
		                         // obtain path (one of them)
	}
	if (size > 65535) {
		lzfs_pretty_syslog(LOG_WARNING, "path too long !!! - truncate");
		size = 65535;
	}

	ret.resize(size);

	p = node;
	while (p != gMetadata->root && p->parents) {
		fsedge *e = p->parents;
		if (size >= e->nleng) {
			size -= e->nleng;
			std::copy(e->name, e->name + e->nleng, ret.begin() + size);
		} else {
			if (size > 0) {
				std::copy(e->name + (e->nleng - size), e->name + e->nleng, ret.begin());
				size = 0;
			}
		}
		if (size > 0) {
			ret[--size] = '/';
		}
		p = e->parent;
	}
}

uint8_t fs_quota_get_info(uint32_t root_inode, const std::vector<QuotaEntry> &entries,
		std::vector<std::string> &result) {
	std::string info;

	result.clear();
	for (const auto &entry : entries) {
		info.clear();
		if (entry.entryKey.owner.ownerType == QuotaOwnerType::kInode) {
			fsnode *node = fsnodes_id_to_node(entry.entryKey.owner.ownerId);
			if (node) {
				fsnodes_getpath(root_inode, node, info);
			}
		}
		result.push_back(info);
	}
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_quota_set(uint8_t sesflags, uint32_t uid, const std::vector<QuotaEntry> &entries) {
	static const char rigor_name[3] = {'S', 'H', 'U'};
	static const char resource_name[2] = {'I', 'S'};
	static const char owner_name[3] = {'U', 'G', 'I'};

	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	if (sesflags & SESFLAG_READONLY) {
		return LIZARDFS_ERROR_EROFS;
	}
	if (uid != 0 && !(sesflags & SESFLAG_ALLCANCHANGEQUOTA)) {
		return LIZARDFS_ERROR_EPERM;
	}
	for (const QuotaEntry &entry : entries) {
		if(entry.entryKey.owner.ownerType != QuotaOwnerType::kInode) {
			continue;
		}
		fsnode *node = fsnodes_id_to_node(entry.entryKey.owner.ownerId);
		if(!node) {
			return LIZARDFS_ERROR_EINVAL;
		}
	}

	for (const QuotaEntry &entry : entries) {
		const QuotaOwner &owner = entry.entryKey.owner;
		gMetadata->quota_database.set(owner.ownerType, owner.ownerId, entry.entryKey.rigor,
		                              entry.entryKey.resource, entry.limit);
		gMetadata->quota_database.removeEmpty(owner.ownerType, owner.ownerId);
		fs_changelog(ts, "SETQUOTA(%c,%c,%c,%" PRIu32 ",%" PRIu64 ")",
		             rigor_name[(int)entry.entryKey.rigor],
		             resource_name[(int)entry.entryKey.resource], owner_name[(int)owner.ownerType],
		             uint32_t{owner.ownerId}, uint64_t{entry.limit});
	}
	return LIZARDFS_STATUS_OK;
}
#endif

uint8_t fs_apply_setquota(char rigor, char resource, char owner_type, uint32_t owner_id,
		uint64_t limit) {
	QuotaRigor quotaRigor = QuotaRigor::kSoft;
	QuotaResource quotaResource = QuotaResource::kSize;
	QuotaOwnerType quotaOwnerType = QuotaOwnerType::kUser;
	bool valid = true;
	valid &= decodeChar("SH", {QuotaRigor::kSoft, QuotaRigor::kHard}, rigor, quotaRigor);
	valid &=
	    decodeChar("SI", {QuotaResource::kSize, QuotaResource::kInodes}, resource, quotaResource);
	valid &=
	    decodeChar("UGI", {QuotaOwnerType::kUser, QuotaOwnerType::kGroup, QuotaOwnerType::kInode},
	               owner_type, quotaOwnerType);
	if (!valid) {
		return LIZARDFS_ERROR_EINVAL;
	}
	gMetadata->metaversion++;
	gMetadata->quota_database.set(quotaOwnerType, owner_id, quotaRigor, quotaResource, limit);
	gMetadata->quota_database.removeEmpty(quotaOwnerType, owner_id);
	return LIZARDFS_STATUS_OK;
}

static int fsnodes_find_depth(fsnode *a) {
	int depth = 0;
	while (a) {
		a = a->parents ? a->parents->parent : nullptr;
		++depth;
	}

	return depth;
}

/*! \brief Find common ancestor.
 *
 * Only path starting from first parent is used to find
 * common ancestor.
 * If the nodes are files with many hard links,
 * then it's possible that this function will fail.
 *
 *  \return Pointer to common ancestor.
 */
static fsnode *fsnodes_find_common_ancestor(fsnode *a, fsnode *b) {
	if (!a || !b) {
		return nullptr;
	}

	int depth_a = fsnodes_find_depth(a);
	int depth_b = fsnodes_find_depth(b);

	if (depth_a > depth_b) {
		for(;depth_a > depth_b;--depth_a) {
			assert(a);
			a = a->parents ? a->parents->parent : nullptr;
		}
	} else if (depth_b > depth_a) {
		for(;depth_b > depth_a;--depth_b) {
			assert(b);
			b = b->parents ? b->parents->parent : nullptr;
		}
	}

	while(a && b) {
		if (a == b) return a;

		a = a->parents ? a->parents->parent : nullptr;
		b = b->parents ? b->parents->parent : nullptr;
	}

	return nullptr;
}

static bool fsnodes_test_dir_quota_noparents(fsnode *node,
		const std::initializer_list<std::pair<QuotaResource, int64_t>> &resource_list) {
	if (!node || node->type != TYPE_DIRECTORY) {
		return false;
	}

	const QuotaDatabase::Limits *entry =
	    gMetadata->quota_database.get(QuotaOwnerType::kInode, node->id);
	if (!entry) {
		return false;
	}

	assert(node->data.ddata.stats);
	statsrecord &stats = *node->data.ddata.stats;
	uint64_t limit;

	for (const auto &resource : resource_list) {
		limit = (*entry)[(int)QuotaRigor::kHard][(int)resource.first];
		if (limit <= 0) {
			continue;
		}

		switch (resource.first) {
		case QuotaResource::kInodes:
			if (((uint64_t)stats.inodes + resource.second) > limit) {
				return true;
			}
			break;
		case QuotaResource::kSize:
			if ((stats.size + resource.second) > limit) {
				return true;
			}
			break;
		}
	}

	return false;
}

bool fsnodes_quota_exceeded_ug(uint32_t uid, uint32_t gid,
		const std::initializer_list<std::pair<QuotaResource, int64_t>> &resource_list) {
	return gMetadata->quota_database.exceeds(QuotaOwnerType::kUser, uid, QuotaRigor::kHard, resource_list) ||
	       gMetadata->quota_database.exceeds(QuotaOwnerType::kGroup, gid, QuotaRigor::kHard, resource_list);
}

bool fsnodes_quota_exceeded_ug(fsnode *node,
		const std::initializer_list<std::pair<QuotaResource, int64_t>> &resource_list) {
	if (!node) {
		return false;
	}

	return fsnodes_quota_exceeded_ug(node->uid, node->gid, resource_list);
}

bool fsnodes_quota_exceeded_dir(fsnode *node,
		const std::initializer_list<std::pair<QuotaResource, int64_t>> &resource_list) {
	if (!node) {
		return false;
	}

	if (fsnodes_test_dir_quota_noparents(node, resource_list)) {
		return true;
	}

	if (node->type == TYPE_DIRECTORY) {
		// Directory can have only one parent, so we get rid of recursion.
		for (fsedge *e = node->parents; e && e->parent; e = e->parent->parents) {
			if (fsnodes_test_dir_quota_noparents(e->parent, resource_list)) {
				return true;
			}
		}
	} else {
		for (fsedge *e = node->parents; e; e = e->nextparent) {
			if (fsnodes_quota_exceeded_dir(e->parent, resource_list)) {
				return true;
			}
		}
	}

	return false;
}

bool fsnodes_quota_exceeded_dir(fsnode *node, fsnode* prev_node,
		const std::initializer_list<std::pair<QuotaResource, int64_t>> &resource_list) {
	assert(node->type == TYPE_DIRECTORY && prev_node->type == TYPE_DIRECTORY);

	// Because nodes are directories fsnodes_find_common_ancestor
	// is guaranteed to work properly.
	fsnode *common = fsnodes_find_common_ancestor(prev_node, node);
	if (node == common) {
		return false;
	}

	if (fsnodes_test_dir_quota_noparents(node, resource_list)) {
		return true;
	}

	// node is directory so it has only one parent.
	for (fsedge *e = node->parents; e && e->parent; e = e->parent->parents) {
		if (e->parent == common) {
			return false;
		}

		if (fsnodes_test_dir_quota_noparents(e->parent, resource_list)) {
			return true;
		}
	}

	return false;
}

bool fsnodes_quota_exceeded(fsnode *node,
		const std::initializer_list<std::pair<QuotaResource, int64_t>> &resource_list) {
	return fsnodes_quota_exceeded_ug(node, resource_list) ||
	       fsnodes_quota_exceeded_dir(node, resource_list);
}

void fsnodes_quota_update(fsnode *node,
		const std::initializer_list<std::pair<QuotaResource, int64_t>> &resource_list) {
	for (const auto &resource : resource_list) {
		if (resource.second == 0) {
			continue;
		}
		gMetadata->quota_database.update(QuotaOwnerType::kUser, node->uid, QuotaRigor::kUsed,
		                                 resource.first, resource.second);
		gMetadata->quota_database.update(QuotaOwnerType::kGroup, node->gid, QuotaRigor::kUsed,
		                                 resource.first, resource.second);
	}
}

void fsnodes_quota_remove(QuotaOwnerType owner_type, uint32_t owner_id) {
	gMetadata->quota_database.remove(owner_type, owner_id);
}

void fsnodes_quota_adjust_space(fsnode * /*node*/, uint64_t & /*total_space*/,
		uint64_t & /*available_space*/) {
}
