/*
   Copyright 2013-2014 EditShare, 2013-2016 Skytechnology sp. z o.o.

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
#include <tuple>
#include <vector>

#include "common/access_control_list.h"
#include "common/massert.h"
#include "common/serialization_macros.h"

namespace legacy {

class ExtendedAcl {
public:
	typedef uint8_t AccessMask;
	enum class EntryType : uint8_t { kNamedUser, kNamedGroup };

	struct Entry {
		Entry() {
		}
		Entry(uint32_t id, EntryType type, AccessMask mask) : id(id), type(type), mask(mask) {
		}

		bool operator==(const Entry &other) const {
			return std::make_tuple(type, id, mask) ==
			       std::make_tuple(other.type, other.id, other.mask);
		}
		uint32_t serializedSize() const {
			return ::serializedSize(bool(), id, mask);
		}
		void serialize(uint8_t **destination) const {
			::serialize(destination, (type == EntryType::kNamedUser), id, mask);
		}
		void deserialize(const uint8_t **source, uint32_t &bytesLeftInBuffer) {
			bool isNamedUser;
			::deserialize(source, bytesLeftInBuffer, isNamedUser, id, mask);
			type = (isNamedUser ? EntryType::kNamedUser : EntryType::kNamedGroup);
		}

		uint32_t id;
		EntryType type;
		AccessMask mask;
	};

	ExtendedAcl() : owningGroupMask_(0) {
	}

	ExtendedAcl(const ::AccessControlList &acl) {
		operator=(acl);
	}

	ExtendedAcl &operator=(const ::AccessControlList &acl) {
		owningGroupMask_ = acl.getEntry(::AccessControlList::kGroup, 0).access_rights;

		list_.clear();
		for(const auto &entry : acl) {
			if (entry.type == ::AccessControlList::kNamedUser) {
				list_.push_back(Entry(entry.id, EntryType::kNamedUser, entry.access_rights));
			}
			if (entry.type == ::AccessControlList::kNamedGroup) {
				list_.push_back(Entry(entry.id, EntryType::kNamedGroup, entry.access_rights));
			}
		}

		return *this;
	}

	explicit operator ::AccessControlList() const {
		::AccessControlList acl;

		for (const auto &entry : list_) {
			if (entry.type == EntryType::kNamedUser) {
				acl.setEntry(::AccessControlList::kNamedUser, entry.id, entry.mask);
			}
			if (entry.type == EntryType::kNamedGroup) {
				acl.setEntry(::AccessControlList::kNamedGroup, entry.id, entry.mask);
			}
		}
		acl.setEntry(::AccessControlList::kGroup, 0, owningGroupMask_);

		return acl;
	}

	LIZARDFS_DEFINE_SERIALIZE_METHODS(owningGroupMask_, list_);

private:
	AccessMask owningGroupMask_;
	std::vector<Entry> list_;
};

class AccessControlList {
public:
	LIZARDFS_CREATE_EXCEPTION_CLASS(IncorrectStringRepresentationException, Exception);

	AccessControlList() {
	}
	AccessControlList(const AccessControlList &acl) {
		*this = acl;
	}
	AccessControlList(AccessControlList &&) = default;

	AccessControlList(const ::AccessControlList& acl) {
		operator=(acl);
	}

	AccessControlList &operator=(const AccessControlList &acl) {
		mode = acl.mode;
		if (acl.extendedAcl) {
			extendedAcl.reset(new ExtendedAcl(*acl.extendedAcl));
		}
		return *this;
	}

	AccessControlList &operator=(AccessControlList &&) = default;

	AccessControlList &operator=(const ::AccessControlList &acl) {
		if (!acl.minimalAcl()) {
			extendedAcl.reset(new ExtendedAcl(acl));
		} else {
			extendedAcl.reset();
		}

		mode = acl.getMode();

		return *this;
	}

	explicit operator ::AccessControlList() const {
		::AccessControlList acl;

		if (extendedAcl) {
			acl = (::AccessControlList) * extendedAcl;
		}
		acl.setMode(mode);

		return acl;
	}

	LIZARDFS_DEFINE_SERIALIZE_METHODS(mode, extendedAcl);

	std::unique_ptr<ExtendedAcl> extendedAcl;
	uint16_t mode;
};

}  // legacy
