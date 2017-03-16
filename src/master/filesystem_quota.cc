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

#include "common/event_loop.h"
#include "common/small_vector.h"
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

	FSNodeDirectory *root_node = fsnodes_id_to_node_verify<FSNodeDirectory>(root_inode);

	auto it = std::remove_if(results.begin(), results.end(), [root_node](const QuotaEntry &entry) {
		if (entry.entryKey.owner.ownerType == QuotaOwnerType::kInode) {
			FSNode *node = fsnodes_id_to_node(entry.entryKey.owner.ownerId);
			if (!node) {
				return true;
			}
			return !fsnodes_isancestor(root_node, node);
		}
		return false;
	});
	results.erase(it, results.end());
}

uint8_t fs_quota_get_all(const FsContext &context, std::vector<QuotaEntry> &results) {
	if (context.uid() != 0 && !(context.sesflags() & SESFLAG_ALLCANCHANGEQUOTA)) {
		return LIZARDFS_ERROR_EPERM;
	}
	results = gMetadata->quota_database.getEntriesWithStats();

	for (auto &entry : results) {
		if (entry.entryKey.owner.ownerType != QuotaOwnerType::kInode ||
		    entry.entryKey.rigor != QuotaRigor::kUsed) {
			continue;
		}

		FSNodeDirectory *node = fsnodes_id_to_node<FSNodeDirectory>(entry.entryKey.owner.ownerId);
		if (!node || node->type != FSNode::kDirectory) {
			continue;
		}

		switch (entry.entryKey.resource) {
		case QuotaResource::kSize:
			entry.limit = node->stats.size;
			break;
		case QuotaResource::kInodes:
			entry.limit = node->stats.inodes;
			break;
		}
	}

	fs_remove_invisible_quota_entries(context.rootinode(), results);

	return LIZARDFS_STATUS_OK;
}

uint8_t fs_quota_get(const FsContext &context,
		const std::vector<QuotaOwner> &owners, std::vector<QuotaEntry> &results) {
	std::vector<QuotaEntry> tmp;
	FSNodeDirectory *node;
	for (const QuotaOwner &owner : owners) {
		if (context.uid() != 0 && !(context.sesflags() & SESFLAG_ALLCANCHANGEQUOTA)) {
			switch (owner.ownerType) {
			case QuotaOwnerType::kUser:
				if (context.uid() != owner.ownerId) {
					return LIZARDFS_ERROR_EPERM;
				}
				break;
			case QuotaOwnerType::kGroup:
				if (context.gid() != owner.ownerId && !(context.sesflags() & SESFLAG_IGNOREGID)) {
					return LIZARDFS_ERROR_EPERM;
				}
				break;
			case QuotaOwnerType::kInode:
				node = fsnodes_id_to_node<FSNodeDirectory>(owner.ownerId);
				if (!node || node->type != FSNode::kDirectory) {
					return LIZARDFS_ERROR_EINVAL;
				}
				if (node->uid != context.uid() || (node->gid != context.gid() && !(context.sesflags() & SESFLAG_IGNOREGID))) {
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
					node = fsnodes_id_to_node<FSNodeDirectory>(owner.ownerId);
					assert(node);
					tmp.push_back({{owner, rigor, QuotaResource::kInodes},
					               (uint64_t)node->stats.inodes});
					tmp.push_back({{owner, rigor, QuotaResource::kSize},
					               (uint64_t)node->stats.size});
					continue;
				}
				for (auto resource : {QuotaResource::kInodes, QuotaResource::kSize}) {
					tmp.push_back({{owner, rigor, resource}, (*result)[(int)rigor][(int)resource]});
				}
			}
		}
	}

	fs_remove_invisible_quota_entries(context.rootinode(), tmp);
	results = std::move(tmp);

	return LIZARDFS_STATUS_OK;
}

static void fsnodes_getpath(uint32_t root_inode, FSNode *node, std::string &ret) {
	std::string::size_type size;
	FSNode *p;

	if (node->id == root_inode) {
		ret = "/";
		return;
	}

	p = node;
	size = 0;
	while (p != gMetadata->root && !p->parent.empty() && p->id != root_inode) {
		// get first parent
		FSNodeDirectory *parent = fsnodes_id_to_node_verify<FSNodeDirectory>(p->parent[0]);
		size += parent->getChildName(p).length() + 1;
		p = parent;
	}
	if (size > 65535) {
		lzfs_pretty_syslog(LOG_WARNING, "path too long !!! - truncate");
		size = 65535;
	}

	ret.resize(size);

	p = node;
	while (p != gMetadata->root && !p->parent.empty()) {
		FSNodeDirectory *parent = fsnodes_id_to_node_verify<FSNodeDirectory>(p->parent[0]);
		std::string name = parent->getChildName(p);
		if (size >= name.length()) {
			size -= name.length();
			std::copy(name.begin() , name.end(), ret.begin() + size);
		} else {
			if (size > 0) {
				std::copy(name.begin() + (name.length() - size), name.end(), ret.begin());
				size = 0;
			}
		}
		if (size > 0) {
			ret[--size] = '/';
		}
		p = parent;
	}
}

uint8_t fs_quota_get_info(const FsContext &context, const std::vector<QuotaEntry> &entries,
		std::vector<std::string> &result) {
	std::string info;

	result.clear();
	for (const auto &entry : entries) {
		info.clear();
		if (entry.entryKey.owner.ownerType == QuotaOwnerType::kInode) {
			FSNode *node = fsnodes_id_to_node(entry.entryKey.owner.ownerId);
			if (node) {
				fsnodes_getpath(context.rootinode(), node, info);
			}
		}
		result.push_back(info);
	}
	return LIZARDFS_STATUS_OK;
}

uint8_t fs_quota_set(const FsContext &context, const std::vector<QuotaEntry> &entries) {
	static const char rigor_name[3] = {'S', 'H', 'U'};
	static const char resource_name[2] = {'I', 'S'};
	static const char owner_name[3] = {'U', 'G', 'I'};

	uint32_t ts = eventloop_time();
	ChecksumUpdater cu(ts);
	if (context.sesflags() & SESFLAG_READONLY) {
		return LIZARDFS_ERROR_EROFS;
	}
	if (context.uid() != 0 && !(context.sesflags() & SESFLAG_ALLCANCHANGEQUOTA)) {
		return LIZARDFS_ERROR_EPERM;
	}
	for (const QuotaEntry &entry : entries) {
		if(entry.entryKey.owner.ownerType != QuotaOwnerType::kInode) {
			continue;
		}
		FSNode *node = fsnodes_id_to_node(entry.entryKey.owner.ownerId);
		if(!node) {
			return LIZARDFS_ERROR_EINVAL;
		}
	}

	for (const QuotaEntry &entry : entries) {
		const QuotaOwner &owner = entry.entryKey.owner;
		gMetadata->quota_database.set(owner.ownerType, owner.ownerId, entry.entryKey.rigor,
		                              entry.entryKey.resource, entry.limit);
		gMetadata->quota_database.removeEmpty(owner.ownerType, owner.ownerId);
		gMetadata->quota_checksum = gMetadata->quota_database.checksum();
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
	gMetadata->quota_checksum = gMetadata->quota_database.checksum();
	return LIZARDFS_STATUS_OK;
}

static int fsnodes_find_depth(FSNodeDirectory *a) {
	assert(a);
	int depth = 1;
	while (!a->parent.empty()) {
		a = fsnodes_id_to_node_verify<FSNodeDirectory>(a->parent[0]);
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
static FSNode *fsnodes_find_common_ancestor(FSNodeDirectory *a, FSNodeDirectory *b) {
	if (!a || !b) {
		return nullptr;
	}

	int depth_a = fsnodes_find_depth(a);
	int depth_b = fsnodes_find_depth(b);

	if (depth_a > depth_b) {
		for(;depth_a > depth_b;--depth_a) {
			assert(a && !a->parent.empty());
			a = fsnodes_id_to_node_verify<FSNodeDirectory>(a->parent[0]);
		}
	} else if (depth_b > depth_a) {
		for(;depth_b > depth_a;--depth_b) {
			assert(b && !b->parent.empty());
			b = fsnodes_id_to_node_verify<FSNodeDirectory>(b->parent[0]);
		}
	}

	if (a == b) {
		return a;
	}

	while(!a->parent.empty()) {
		assert(!b->parent.empty());

		a = fsnodes_id_to_node_verify<FSNodeDirectory>(a->parent[0]);
		b = fsnodes_id_to_node_verify<FSNodeDirectory>(b->parent[0]);

		if (a == b) {
			return a;
		}
	}

	return nullptr;
}

static bool fsnodes_test_dir_quota_noparents(FSNode *node,
		const std::initializer_list<std::pair<QuotaResource, int64_t>> &resource_list) {
	if (!node || node->type != FSNode::kDirectory) {
		return false;
	}

	const QuotaDatabase::Limits *entry =
	    gMetadata->quota_database.get(QuotaOwnerType::kInode, node->id);
	if (!entry) {
		return false;
	}

	const statsrecord &stats = static_cast<FSNodeDirectory*>(node)->stats;
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

bool fsnodes_quota_exceeded_ug(FSNode *node,
		const std::initializer_list<std::pair<QuotaResource, int64_t>> &resource_list) {
	if (!node) {
		return false;
	}

	return fsnodes_quota_exceeded_ug(node->uid, node->gid, resource_list);
}

bool fsnodes_quota_exceeded_dir(FSNode *node,
		const std::initializer_list<std::pair<QuotaResource, int64_t>> &resource_list) {
	if (!node) {
		return false;
	}

	if (fsnodes_test_dir_quota_noparents(node, resource_list)) {
		return true;
	}

	if (node->type == FSNode::kDirectory) {
		// Directory can have only one parent, so we get rid of recursion.
		while(!node->parent.empty()) {
			FSNodeDirectory *parent = fsnodes_id_to_node_verify<FSNodeDirectory>(node->parent[0]);
			if (fsnodes_test_dir_quota_noparents(parent, resource_list)) {
				return true;
			}
			node = parent;
		}
	} else {
		for(const auto parent_id : node->parent) {
			FSNodeDirectory *parent = fsnodes_id_to_node_verify<FSNodeDirectory>(parent_id);
			if (fsnodes_quota_exceeded_dir(parent, resource_list)) {
				return true;
			}
		}
	}

	return false;
}

bool fsnodes_quota_exceeded_dir(FSNodeDirectory *node, FSNodeDirectory* prev_node,
		const std::initializer_list<std::pair<QuotaResource, int64_t>> &resource_list) {
	// Because nodes are directories fsnodes_find_common_ancestor
	// is guaranteed to work properly.
	FSNode *common = fsnodes_find_common_ancestor(prev_node, node);
	if (node == common) {
		return false;
	}

	if (fsnodes_test_dir_quota_noparents(node, resource_list)) {
		return true;
	}

	// node is directory so it has only one parent.
	while(!node->parent.empty()) {
		FSNodeDirectory *parent = fsnodes_id_to_node<FSNodeDirectory>(node->parent[0]);

		if (parent == common) {
			return false;
		}

		if (fsnodes_test_dir_quota_noparents(parent, resource_list)) {
			return true;
		}

		node = parent;
	}

	return false;
}

bool fsnodes_quota_exceeded(FSNode *node,
		const std::initializer_list<std::pair<QuotaResource, int64_t>> &resource_list) {
	return fsnodes_quota_exceeded_ug(node, resource_list) ||
	       fsnodes_quota_exceeded_dir(node, resource_list);
}

void fsnodes_quota_update(FSNode *node,
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
	gMetadata->quota_checksum = gMetadata->quota_database.checksum();
}

void fsnodes_quota_adjust_space(FSNode * /*node*/, uint64_t & /*total_space*/,
		uint64_t & /*available_space*/) {
}
