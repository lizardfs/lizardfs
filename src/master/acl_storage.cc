/*
   Copyright 2017 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

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

#include "common/hashfn.h"
#include "master/acl_storage.h"

AclStorage::~AclStorage() {
	#ifndef NDEBUG
		// Assert refcount sanity
		for (auto &p : acl_) {
			p.second.get().second--;
		}
		for (auto &kv : storage_) {
			assert(kv.second == 0);
		}
	#endif
}

size_t AclStorage::Hash::operator()(const RichACL &acl) const {
	//NOTE(konrad.lipinski) the standard denotes bucket_count as implementation-defined;
	// pragmatic implementations (ex. libstdc++) are expected to use a prime number though
	// so there's no need to introduce explicit entropy into initial seed value
	uint64_t seed = 0;
	hashCombine(seed, acl.getOwnerMask());
	hashCombine(seed, acl.getGroupMask());
	hashCombine(seed, acl.getOtherMask());
	hashCombine(seed, acl.getFlags());

	for (const RichACL::Ace &ace : acl) {
		hashCombine(seed, ace.type);
		hashCombine(seed, ace.flags);
		hashCombine(seed, ace.mask);
		hashCombine(seed, ace.id);
	}
	return seed;
}

const RichACL *AclStorage::get(InodeId id) const {
	const auto it = acl_.find(id);
	return it != acl_.end()
		? std::addressof(it->second.get().first)
		: nullptr;
}

void AclStorage::set(InodeId id, RichACL &&acl) {
	const auto it = acl_.find(id);
	KeyValue &kv = ref(std::move(acl));
	if (it == acl_.end()) {
		acl_.insert({id, kv});
	} else {
		unref(it->second);
		it->second = kv;
	}
}

void AclStorage::erase(InodeId id) {
	const auto it = acl_.find(id);
	if (it != acl_.end()) {
		unref(it->second);
		acl_.erase(it);
	}
}

void AclStorage::setMode(InodeId id, uint16_t mode, bool is_dir) {
	const auto it = acl_.find(id);
	if (it != acl_.end()) {
		auto &kv_ref = it->second;
		RichACL acl_with_mode(kv_ref.get().first);
		acl_with_mode.setMode(mode, is_dir);
		if (acl_with_mode != kv_ref.get().first) {
			unref(kv_ref);
			kv_ref = ref(std::move(acl_with_mode));
		}
	}
}

AclStorage::KeyValue &AclStorage::ref(RichACL &&acl) {
	const auto p = storage_.insert(KeyValue(std::move(acl), 0UL));
	auto &kv = *p.first;
	assert(p.second || kv.second > 0);
	kv.second++;
	assert(kv.second > 0);
	return kv;
}

void AclStorage::unref(KeyValue &kv) {
	assert(kv.second > 0);
	kv.second--;
	if (kv.second == 0) {
		const auto it = storage_.find(kv.first);
		assert(it != storage_.end());
		storage_.erase(it);
	}
}
