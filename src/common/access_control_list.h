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

#include <memory>

#include "common/compact_vector.h"
#include "common/exception.h"
#include "common/flat_set.h"
#include "common/serialization_macros.h"

/*! \brief Access Control List
 *
 * This gives interface for accessing and modifying access control list (acl).
 *
 * Selection of containers used internally by this class is directed towards
 * small memory footprint.
 *
 * For more information about acl \see http://www.vanemery.com/Linux/ACL/POSIX_ACL_on_Linux.html
 */
class AccessControlList {
public:
	LIZARDFS_CREATE_EXCEPTION_CLASS(IncorrectStringRepresentationException, Exception);

	typedef uint8_t AccessMask;

	static constexpr uint8_t kMaskUnset = 0xF;

	/* \brief Types of ACL access rights. */
	enum { kNamedUser, kNamedGroup, kUser, kGroup, kOther, kMask, kInvalid };

#pragma pack(push)
#pragma pack(1)
	struct Entry {
		Entry() : id(), type(), access_rights() {
		}

		Entry(uint8_t type, uint32_t id, AccessMask rights)
		    : id(id), type(type), access_rights(rights) {
		}

		bool operator==(const Entry &other) const {
			return type == other.type && id == other.id && access_rights == other.access_rights;
		}

		bool operator<(const Entry &other) const {
			return std::make_tuple((uint8_t)type, id, (uint8_t)access_rights) <
			       std::make_tuple((uint8_t)other.type, other.id, (uint8_t)other.access_rights);
		}

		uint32_t serializedSize() const {
			return ::serializedSize(uint8_t(), id, uint8_t());
		}

		void serialize(uint8_t **destination) const {
			return ::serialize(destination, (uint8_t)type, id, (uint8_t)access_rights);
		}

		void deserialize(const uint8_t **source, uint32_t &bytesLeftInBuffer) {
			uint8_t t, r;
			::deserialize(source, bytesLeftInBuffer, t, id, r);
			type = t;
			access_rights = r;
		}

		uint32_t id;               /*!< Owner id. */
		uint8_t type : 4;          /*!< Access rights type (user, group). */
		uint8_t access_rights : 4; /*!< Access rights mask. */
	};
#pragma pack(pop)

	typedef flat_set<Entry, compact_vector<Entry, uint16_t>>::iterator iterator;
	typedef flat_set<Entry, compact_vector<Entry, uint16_t>>::const_iterator const_iterator;

	AccessControlList() : list_(), basic_permissions_(0xF000) {
	}
	AccessControlList(const AccessControlList &) = default;
	AccessControlList(AccessControlList &&) = default;

	AccessControlList &operator=(const AccessControlList &) = default;
	AccessControlList &operator=(AccessControlList &&) = default;

	/*! \brief Conversion to string.
	 *
	 * The format is eg:
	 * A760 -- minimal ACL - just 'A' + mode in octal
	 * A770/m::6 -- extended acl with only the mask rw- (6 in octal)
	 * A770/u:123:7/m::6 -- the same as before, but with uid 123 having rwx rights
	 * A770/u:123:7/g:166:4/m::6 -- the same as before, but with gid 166 having r-- rights
	 *
	 * \result String representation of acl.
	 */
	std::string toString() const;

	/*! \brief Conversion from string.
	 *
	 * Throws IncorrectStringRepresentationException exception
	 * if valid conversion of text couldn't be performed.
	 *
	 * \param text String representation of acl.
	 * \return Acl object.
	 */
	static AccessControlList fromString(const std::string &text);

	/*! \brief Get Linux inode mode with access rights.
	 *
	 * For minimal acl mode bits represent permission for owner user, owner group and other
	 * (3 bits for each type).
	 *
	 * bit index type
	 * 1         other read access
	 * 2         other write access
	 * 3         other execute
	 * 4         group read access
	 * 5         group write access
	 * 6         group execute
	 * 7         owner read access
	 * 8         owner write access
	 * 9         owner execute rights

	 * If mask entry is available in acl then mask replaces owner group, so
	 * bits represents permissions for owner, mask, other.
	 *
	 * bit index type
	 * 1         other read access
	 * 2         other write access
	 * 3         other execute
	 * 4         mask read access
	 * 5         mask write access
	 * 6         mask execute
	 * 7         owner read access
	 * 8         owner write access
	 * 9         owner execute rights
	 */
	uint16_t getMode() const {
		if (getMaskRights() != kMaskUnset) {
			return (getOtherRights() & 0x7) |
			       ((getMaskRights() & 0x7) << 3) |
			       ((getOwnerRights() & 0x7) << 6);
		}
		return (getOtherRights() & 0x7) |
		       ((getGroupRights() & 0x7) << 3) |
		       ((getOwnerRights() & 0x7) << 6);
	}

	/*! \brief Set Linux inode mode with access rights.
	 *
	 * For minimal acl it sets owner, owner group and other.
	 * For extended acl owner group is replaced with mask.
	 */
	void setMode(uint16_t mode) {
		setOtherRights(mode & 0x7);
		setOwnerRights((mode >> 6) & 0x7);

		if (getMaskRights() == kMaskUnset) {
			setGroupRights((mode >> 3) & 0x7);
		} else {
			setMaskRights((mode >> 3) & 0x7);
		}
	}

	/*! \brief Set access right for specific entry.
	 *
	 * \param type access rights type
	 * \param id owner id
	 * \param access_rights mask with access rights (rwx)
	*/
	void setEntry(uint8_t type, uint32_t id, AccessMask access_rights) {
		assert(access_rights <= 7);

		switch (type) {
		case kNamedUser:
		case kNamedGroup: {
			auto it = list_.lower_bound(Entry(type, id, 0));
			if (it != list_.end()) {
				if (it->type == type && it->id == id) {
					it->access_rights = access_rights;
					return;
				}
			}

			if (list_.size() < list_.max_size()) {
				list_.insert(Entry(type, id, access_rights));
			}
			break;
		}
		case kUser:
			setOwnerRights(access_rights);
			break;
		case kGroup:
			setGroupRights(access_rights);
			break;
		case kOther:
			setOtherRights(access_rights);
			break;
		case kMask:
			setMaskRights(access_rights);
			break;
		default:
			assert(!"Invalid ACL access rights type");
		}
	}

	/*! \brief Remove entry with access rights.
	 *
	 * \param type access rights type
	 * \param id owner id
	*/
	void removeEntry(uint8_t type, uint32_t id) {
		switch (type) {
		case kNamedUser:
		case kNamedGroup: {
			auto it = list_.lower_bound(Entry(type, id, 0));
			if (it != list_.end() && it->type == type && it->id == id) {
				list_.erase(it);
			}
			break;
		}
		case kUser:
			setOwnerRights(0x0);
			break;
		case kGroup:
			setGroupRights(0x0);
			break;
		case kOther:
			setOtherRights(0x0);
			break;
		case kMask:
			setMaskRights(0xF);
			break;
		default:
			assert(!"Invalid ACL access rights type");
		}
	}

	/*! \brief Get entry.
	 *
	 * \param type access rights type
	 * \param id owner id
	*/
	Entry getEntry(uint8_t type, uint32_t id) const {
		switch (type) {
		case kNamedUser:
		case kNamedGroup: {
			auto it = list_.lower_bound(Entry(type, id, 0));
			if (it != list_.end() && it->type == type && it->id == id) {
				return *it;
			}
			break;
		}
		case kUser:
			return Entry(kUser, 0xFFFFFFFFU, getOwnerRights());
		case kGroup:
			return Entry(kGroup, 0xFFFFFFFFU, getGroupRights());
		case kOther:
			return Entry(kOther, 0xFFFFFFFFU, getOtherRights());
		case kMask:
			if (getMaskRights() != kMaskUnset) {
				return Entry(kMask, 0xFFFFFFFFU, getMaskRights());
			}
			break;
		default:
			assert(!"Invalid ACL access rights type");
		}

		return Entry(kInvalid, 0xFFFFFFFFU, 0);
	}

	/*! \brief Get effective rights for user.
	 *
	 * \param owner_uid Owner id for resource.
	 * \param owner_gid Owner group id for resource.
	 * \param uid User id to check access for.
	 * \param groups_container Container with ids of groups that user belongs to.
	 * \return Mask with access rights for this user (rwx)
	*/
	template <typename Groups>
	AccessMask getEffectiveRights(uint32_t owner_uid, uint32_t owner_gid, uint32_t uid,
	                              const Groups &group_container) const {
		if (uid == owner_uid) {
			return getEntry(kUser, 0).access_rights;
		}

		auto user_entry = getEntry(kNamedUser, uid);
		if (user_entry.type != kInvalid) {
			return applyMask(user_entry.access_rights);
		}

		AccessMask access_rights = 0;
		bool found = false;
		auto it = std::find(group_container.begin(), group_container.end(), owner_gid);
		if (it != group_container.end()) {
			auto group_entry = getEntry(kGroup, 0);
			access_rights = group_entry.access_rights;
			found = true;
		}

		for (const auto &id : group_container) {
			auto group_entry = getEntry(kNamedGroup, id);
			if (group_entry.type != kInvalid) {
				access_rights |= group_entry.access_rights;
				found = true;
			}
		}

		if (found) {
			return applyMask(access_rights);
		}

		auto other_entry = getEntry(kOther, 0);
		return other_entry.access_rights;
	}

	/*! \brief Returns true if this is minimal acl. */
	bool minimalAcl() const {
		return (getMaskRights() == kMaskUnset) && list_.empty();
	}

	/*! \brief If mask is available then it applies mask to access right.
	 * \param access_rights Input access rights.
	 * \return Access rights with applied mask.
	 */
	AccessMask applyMask(AccessMask access_rights) const {
		if (getMaskRights() != kMaskUnset) {
			return access_rights & getMaskRights();
		}
		return access_rights;
	}

	iterator begin() {
		return list_.begin();
	}

	iterator end() {
		return list_.end();
	}

	const_iterator begin() const {
		return list_.begin();
	}

	const_iterator end() const {
		return list_.end();
	}

	bool operator==(const AccessControlList &other) const {
		return basic_permissions_ == other.basic_permissions_ && list_ == other.list_;
	}

	bool operator!=(const AccessControlList &other) const {
		return !(*this == other);
	}

	bool operator<(const AccessControlList &other) const {
		return basic_permissions_ < other.basic_permissions_ ||
		       (basic_permissions_ == other.basic_permissions_ && list_ < other.list_);
	}

	LIZARDFS_DEFINE_SERIALIZE_METHODS(basic_permissions_, list_);

protected:
	uint8_t getOwnerRights() const {
		return (basic_permissions_ >> 8) & 0xF;
	}
	uint8_t getGroupRights() const {
		return (basic_permissions_ >> 4) & 0xF;
	}
	uint8_t getOtherRights() const {
		return basic_permissions_ & 0xF;
	}
	uint8_t getMaskRights() const {
		return basic_permissions_ >> 12;
	}

	void setOwnerRights(uint8_t value) {
		basic_permissions_ = (basic_permissions_ & 0xF0FF) | (static_cast<uint16_t>(value & 0xF) << 8);
	}
	void setGroupRights(uint8_t value) {
		basic_permissions_ = (basic_permissions_ & 0xFF0F) | (static_cast<uint16_t>(value & 0xF) << 4);
	}
	void setOtherRights(uint8_t value) {
		basic_permissions_ = (basic_permissions_ & 0xFFF0) | (value & 0xF);
	}
	void setMaskRights(uint8_t value) {
		basic_permissions_ = (basic_permissions_ & 0x0FFF) | (static_cast<uint16_t>(value & 0xF) << 12);
	}

	flat_set<Entry, compact_vector<Entry, uint16_t>>
	    list_;                   /*!< Container with named user and named group access rights. */
	uint16_t basic_permissions_; /*!< Bit storage for other, owner group, owner, and mask access
	                                rights. Each entry takes 4 bits. If the entry is not available
	                                then it is marked as 0xF. */
};
