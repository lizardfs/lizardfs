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

#pragma once

#include "common/platform.h"

#include <unordered_map>

#include "common/richacl.h"

/*!
 * \brief A class aggregating ACL storage and inode->acl maps, deduplication included.
 */
class AclStorage {
public:
	AclStorage(const AclStorage&) = delete;
	AclStorage& operator=(const AclStorage&) = delete;
	AclStorage(AclStorage&&) = delete;
	AclStorage& operator=(AclStorage&&) = delete;

	AclStorage() = default;

	/*!
	 * \brief Assert state sanity.
	 */
	~AclStorage();

	/*!
	 * \brief Inode identifier type
	 */
	typedef uint32_t InodeId;

	/*!
	 * \brief Find ACL for an inode.
	 *
	 * \param id inode id
	 * \return ACL mapped to id or nullptr if none
	 */
	const RichACL *get(InodeId id) const;

	/*!
	 * \brief Set ACL for an inode.
	 *
	 * \param id inode id
	 * \param acl ACL to be set
	 */
	void set(InodeId id, RichACL &&acl);

	/*!
	 * \brief Erase ACL for an inode.
	 *
	 * \param id inode id
	 */
	void erase(InodeId id);

	/*!
	 * \brief Set mode of ACL of an inode (if any).
	 *
	 * \param id inode id
	 * \param mode mode to be set
	 * \param is_dir true if inode is a directory
	 */
	void setMode(InodeId id, uint16_t mode, bool is_dir);
private:
	struct Hash {
		size_t operator()(const RichACL &acl) const;
	};

	typedef std::unordered_map<RichACL, unsigned long, Hash> AclToRefCountMap;
	typedef AclToRefCountMap::value_type KeyValue;
	typedef std::unordered_map<InodeId, std::reference_wrapper<KeyValue>> InodeToKVMap;

	/*!
	 * \brief Insert an ACL into permanent storage.
	 *
	 * Insert a key-value (ACL, reference count) into storage or increase
	 * reference count of one already present.
	 *
	 * \param acl key
	 * \return key-value pair from storage
	 */
	KeyValue &ref(RichACL &&acl);

	/*!
	 * \brief Decrease reference count of an ACL.
	 *
	 * Decrease reference count of a key-value pair from storage and remove
	 * it from storage if count reaches zero.
	 *
	 * \param kv key-value pair from storage
	 */
	void unref(KeyValue &kv);

	AclToRefCountMap storage_;
	InodeToKVMap acl_;
};
