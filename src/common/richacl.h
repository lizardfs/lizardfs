/*
   Copyright 2017 Skytechnology sp. z o.o.

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

#include <sys/stat.h>
#include <algorithm>
#include <cstdint>
#include <string>
#include <tuple>
#include <vector>
#include "common/exception.h"
#include "common/access_control_list.h"
#include "common/serialization_macros.h"

class RichACL {
public:
	/* acl mode flags */
	static constexpr uint16_t AUTO_INHERIT = 0x01;
	static constexpr uint16_t PROTECTED = 0x02;
	static constexpr uint16_t DEFAULTED = 0x04;
	static constexpr uint16_t WRITE_THROUGH = 0x40;
	static constexpr uint16_t MASKED = 0x80;
	static constexpr uint16_t AUTO_SET_MODE = 0x20;

	LIZARDFS_CREATE_EXCEPTION_CLASS(FormatException, Exception);

	struct Ace {
		static constexpr uint32_t kInvalidId = 0xFFFFFFFF;

		/* type values */
		static constexpr uint32_t ACCESS_ALLOWED_ACE_TYPE = 0x0000;
		static constexpr uint32_t ACCESS_DENIED_ACE_TYPE = 0x0001;

		/* flags bitflags */
		static constexpr uint32_t FILE_INHERIT_ACE = 0x0001;
		static constexpr uint32_t DIRECTORY_INHERIT_ACE = 0x0002;
		static constexpr uint32_t NO_PROPAGATE_INHERIT_ACE = 0x0004;
		static constexpr uint32_t INHERIT_ONLY_ACE = 0x0008;
		static constexpr uint32_t SUCCESSFUL_ACCESS_ACE_FLAG = 0x00000010;
		static constexpr uint32_t FAILED_ACCESS_ACE_FLAG = 0x00000020;
		static constexpr uint32_t IDENTIFIER_GROUP = 0x0040;
		static constexpr uint32_t INHERITED_ACE = 0x0080;  // non nfs4
		static constexpr uint32_t SPECIAL_WHO = 0x0100;    // lizardfs

		/* mask bitflags */
		static constexpr uint32_t READ_DATA = 0x00000001;
		static constexpr uint32_t LIST_DIRECTORY = 0x00000001;
		static constexpr uint32_t WRITE_DATA = 0x00000002;
		static constexpr uint32_t ADD_FILE = 0x00000002;
		static constexpr uint32_t APPEND_DATA = 0x00000004;
		static constexpr uint32_t ADD_SUBDIRECTORY = 0x00000004;
		static constexpr uint32_t READ_NAMED_ATTRS = 0x00000008;
		static constexpr uint32_t WRITE_NAMED_ATTRS = 0x00000010;
		static constexpr uint32_t EXECUTE = 0x00000020;
		static constexpr uint32_t DELETE_CHILD = 0x00000040;
		static constexpr uint32_t READ_ATTRIBUTES = 0x00000080;
		static constexpr uint32_t WRITE_ATTRIBUTES = 0x00000100;
		static constexpr uint32_t WRITE_RETENTION = 0x00000200;
		static constexpr uint32_t WRITE_RETENTION_HOLD = 0x00000400;
		static constexpr uint32_t DELETE = 0x00010000;
		static constexpr uint32_t READ_ACL = 0x00020000;
		static constexpr uint32_t WRITE_ACL = 0x00040000;
		static constexpr uint32_t WRITE_OWNER = 0x00080000;
		static constexpr uint32_t SYNCHRONIZE = 0x00100000;

		/* special id values */
		static constexpr uint32_t OWNER_SPECIAL_ID = 0;
		static constexpr uint32_t GROUP_SPECIAL_ID = 1;
		static constexpr uint32_t EVERYONE_SPECIAL_ID = 2;

		static constexpr uint32_t POSIX_MODE_READ = READ_DATA | LIST_DIRECTORY;
		static constexpr uint32_t POSIX_MODE_WRITE =
		    WRITE_DATA | ADD_FILE | APPEND_DATA | ADD_SUBDIRECTORY | DELETE_CHILD;
		static constexpr uint32_t POSIX_MODE_EXEC = EXECUTE;
		static constexpr uint32_t POSIX_MODE_ALL =
		    POSIX_MODE_READ | POSIX_MODE_WRITE | POSIX_MODE_EXEC;

		static constexpr uint32_t POSIX_ALWAYS_ALLOWED = SYNCHRONIZE | READ_ATTRIBUTES | READ_ACL;

		static constexpr uint32_t POSIX_OWNER_ALLOWED = WRITE_ATTRIBUTES | WRITE_OWNER | WRITE_ACL;

		static constexpr uint32_t INHERITANCE_FLAGS = FILE_INHERIT_ACE | DIRECTORY_INHERIT_ACE |
		                                              NO_PROPAGATE_INHERIT_ACE | INHERIT_ONLY_ACE |
		                                              INHERITED_ACE;

		static constexpr uint32_t VALID_MASK =
		    READ_DATA | LIST_DIRECTORY | WRITE_DATA | ADD_FILE | APPEND_DATA | ADD_SUBDIRECTORY |
		    READ_NAMED_ATTRS | WRITE_NAMED_ATTRS | EXECUTE | DELETE_CHILD | READ_ATTRIBUTES |
		    WRITE_ATTRIBUTES | WRITE_RETENTION | WRITE_RETENTION_HOLD | DELETE | READ_ACL |
		    WRITE_ACL | WRITE_OWNER | SYNCHRONIZE;

		Ace() : type(), flags(), mask(), id() {
		}

		Ace(uint32_t type, uint32_t flags, uint32_t mask, uint32_t id)
		    : type(type), flags(flags), mask(mask), id(id) {
		}

		bool operator==(const Ace &other) const {
			return std::make_tuple(type, flags, mask, id) ==
				std::make_tuple(other.type, other.flags, other.mask, other.id);
		}

		bool operator!=(const Ace &other) const {
			return !(*this == other);
		}

		bool operator<(const Ace &other) const {
			return std::make_tuple(type, flags, mask, id) <
				std::make_tuple(other.type, other.flags, other.mask, other.id);
		}

		std::string toString() const;

		bool isAllow() const {
			return type == ACCESS_ALLOWED_ACE_TYPE;
		}

		bool isDeny() const {
			return type == ACCESS_DENIED_ACE_TYPE;
		}

		bool isOwner() const {
			return (flags & SPECIAL_WHO) && id == OWNER_SPECIAL_ID;
		}

		bool isGroup() const {
			return (flags & SPECIAL_WHO) && id == GROUP_SPECIAL_ID;
		}

		bool isEveryone() const {
			return (flags & SPECIAL_WHO) && id == EVERYONE_SPECIAL_ID;
		}

		bool isUnixUser() const {
			return !(flags & SPECIAL_WHO) && !(flags & IDENTIFIER_GROUP);
		}

		bool isUnixGroup() const {
			return !(flags & SPECIAL_WHO) && (flags & IDENTIFIER_GROUP);
		}

		bool isInheritable() const {
			return flags & (FILE_INHERIT_ACE | DIRECTORY_INHERIT_ACE);
		}

		bool isInheritOnly() const {
			return flags & INHERIT_ONLY_ACE;
		}

		bool isSameIdentifier(const Ace &ace) const {
			return !(((flags ^ ace.flags) & (SPECIAL_WHO | IDENTIFIER_GROUP)) || id != ace.id);
		}

		bool inheritsToDirectory() const {
			if (flags & DIRECTORY_INHERIT_ACE) {
				return true;
			}
			if ((flags & FILE_INHERIT_ACE) && !(flags & NO_PROPAGATE_INHERIT_ACE)) {
				return true;
			}
			return false;
		}

		uint32_t serializedSize() const {
			return ::serializedSize(uint8_t(), uint16_t(), uint32_t(), uint32_t());
		}

		void serialize(uint8_t** destination) const {
			::serialize(destination, (uint8_t)type, (uint16_t)flags, (uint32_t)mask, id);
		}

		void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer) {
			uint8_t tmp_type;
			uint16_t tmp_flags;
			uint32_t tmp_mask;
			::deserialize(source, bytesLeftInBuffer, tmp_type, tmp_flags, tmp_mask, id);
			type = tmp_type;
			flags = tmp_flags;
			mask = tmp_mask;
		}

		uint32_t type : 2;
		uint32_t flags : 9;
		uint32_t mask : 21;
		uint32_t id;
	};

	typedef std::vector<Ace> AceList;
	typedef AceList::iterator iterator;
	typedef AceList::const_iterator const_iterator;
	typedef AceList::size_type size_type;

	RichACL() : owner_mask_(), group_mask_(), other_mask_(), flags_() {
	}

	bool operator==(const RichACL &other) const {
		return std::make_tuple(owner_mask_, group_mask_, other_mask_, flags_) ==
			std::make_tuple(other.owner_mask_, other.group_mask_, other.other_mask_, other.flags_)
			&& ace_list_ == other.ace_list_;
	}

	bool operator!=(const RichACL &other) const {
		return !(*this == other);
	}

	std::string toString() const;

	/*!
	 * \brief Load RichACL from string format.
	 * RichACL is encoded as follows:
	 *  FLAGS|OWNER_MASK|GROUP_MASK|EVERYONE_MASK|ACE[/ACE/...]
	 * while every ACE is encoded as:
	 *  MASK:FLAGS:TYPE:IDENTIFIER
	 *
	 * Example:
	 * Parsing "|rwxcC|rwxcC|rwxcC|r::D:u1000/wxC::A:g1000/rwxcC::A:E/"
	 * will result in
	 *  user:1000:r------------::deny
	 *  group:1000:rw-x-----C---::allow
	 *  EVERYONE@:rw-x-----C---::allow
	 */
	static RichACL fromString(const std::string &str);

	void insert(const Ace &ace) {
		ace_list_.push_back(ace);
	}

	size_type size() const noexcept {
		return ace_list_.size();
	}

	iterator begin() {
		return ace_list_.begin();
	}

	iterator end() {
		return ace_list_.end();
	}

	const_iterator begin() const {
		return ace_list_.begin();
	}

	const_iterator end() const {
		return ace_list_.end();
	}

	void setOwnerMask(uint32_t mask) {
		owner_mask_ = mask;
	}

	void setGroupMask(uint32_t mask) {
		group_mask_ = mask;
	}

	void setOtherMask(uint32_t mask) {
		other_mask_ = mask;
	}

	void setFlags(uint16_t flags) {
		flags_ = flags;
	}

	uint32_t getOwnerMask() const {
		return owner_mask_;
	}

	uint32_t getGroupMask() const {
		return group_mask_;
	}

	uint32_t getOtherMask() const {
		return other_mask_;
	}

	uint16_t getFlags() const {
		return flags_;
	}

	bool isAutoInherit() const {
		return flags_ & AUTO_INHERIT;
	}

	bool isProtected() const {
		return flags_ & PROTECTED;
	}

	bool isMasked() const {
		return flags_ & MASKED;
	}

	bool isAutoSetMode() const {
		return flags_ & AUTO_SET_MODE;
	}

	bool isSameMode(uint16_t mode, bool is_dir) const;
	void setMode(uint16_t mode, bool is_dir);
	uint16_t getMode() const;

	/*! \brief RichACL permission check algorithm.
	 *
	 * \param mask Mask with requested access.
	 * \param owner_uid Inode owner id.
	 * \param owner_gid Inode owneg gid.
	 * \param uid Id of user making the check.
	 * \param group_container List of group ids that user making the check belongs to.
	 *
	 * \return true if all the requested permissions are granted.
	 */
	template <class GroupsContainer>
	bool checkPermission(uint32_t mask, uint32_t owner_uid, uint32_t owner_gid, uint32_t uid,
	                     const GroupsContainer &group_container);

	/*! \brief Apply masks to the acl
	 *
	 * Transform RichACL so that the standard NFSv4 permission check algorithm (which
	 * is not aware of file masks) will compute the same access decisions as the
	 * richacl permission check algorithm (which looks at the acl and the file
	 * masks).
	 *
	 * The algorithm is described in draft-gruenbacher-nfsv4-acls-in-posix-00.
	 *
	 * \param owner inode owner id.
	 */
	void applyMasks(uint32_t owner);

	/*! \brief Append Aces to RichACL that give the same permissions as \param posix_acl.
	 *
	 * \param posix_acl Posix ACL that should be converted to RichACL.
	 * \param is_dir True if node with RichACL is directory.
	 */
	void appendPosixACL(const AccessControlList &posix_acl, bool is_dir);

	/*! \brief Append inheritable only Aces to richacl that give the same permissions as \param posix_acl.
	 *
	 * \param posix_acl Posix ACL that should be converted to RichACL.
	 */
	void appendDefaultPosixACL(const AccessControlList &posix_acl);

	/*! \brief Convert RichACL to PosixACL.
	 *
	 * Note: It's not always possible to exactly convert RichACL to Posix ACL.
	 *
	 * \return Pair consisting of bool value set to true if posix acl is non-empty
	 *         and converted posix acl.
	 */
	std::pair<bool, AccessControlList> convertToPosixACL() const;

	/*! \brief Convert RichACL to default PosixACL (only inheritable Aces).
	 *
	 * Note: It's not always possible to exactly convert RichACL to Posix ACL.
	 *
	 * \return Pair consisting of bool value set to true if posix acl is non-empty
	 *         and converted posix acl.
	 */
	std::pair<bool, AccessControlList> convertToDefaultPosixACL() const;

	/*! \brief Convert inheritable Aces to double entries - permission Ace and inheritable only Ace.
	 */
	void createExplicitInheritance();


	/*! \brief Remove Aces with inherit only flag.
	 *
	 * \param remove_with_flag_set When true remove Aces with flag set. On false remove Aces
	 *                             without inherit only flag.
	 */
	void removeInheritOnly(bool remove_with_flag_set = true);

	/*! \brief Checks if inherit flags are conforming to NFS 4.1 RFC.
	 *
	 * \return true if inherit flags are properly set.
	 */
	bool checkInheritFlags(bool is_directory) const;

	/*! \brief Creates RichACL with permission inhertited from \param dir_acl.
	 *
	 * \param dir_acl RichACL to inherit from.
	 * \param mode Inheriting inode mode field.
	 * \param acl Inherited RichACL.
	 * \param umask File system umask.
	 * \param is_dir True if inheriting inode is directory.
	 * \return True if inherited RichACL is valid.
	 */
	static bool inheritInode(const RichACL &dir_acl, uint16_t &mode, RichACL &acl,
	                         uint16_t umask, bool is_dir);

	/*! \brief Create an acl which corresponds to \param mode
	 *
	 * The resulting acl doesn't have the MASKED flag set.
	 * \param mode File mode.
	 * \param is_dir Should be true if acl is created for directory.
	 */
	static RichACL createFromMode(uint16_t mode, bool is_dir);

	/*! \brief Compute a file mask from the lowest three mode bits.
	 *
	 * \param mode Mode to convert to RichACL permissions.
	 * \return Mask with RichACL permissions.
	 */
	static uint32_t convertMode2Mask(uint16_t mode);

	/*! \brief Compute file permission bits from file masks.
	 *
	 * \param mask Mask with RichACL permissions.
	 * \return File mode.
	 */
	static uint16_t convertMask2Mode(uint32_t mask);

	/*! \brief Check if \param acl is equivalent to \param mode file mask
	 *
	 * \param acl RichACL to check.
	 * \param mode File mode mask to check
	 * \param is_dir True if inode that acl belongs to is a directory.
	 * \return True is acl is equivalent to mode.
	 */
	static bool equivMode(const RichACL &acl, uint16_t &mode, bool is_dir);

	LIZARDFS_DEFINE_SERIALIZE_METHODS(owner_mask_, group_mask_, other_mask_, flags_, ace_list_)

protected:
	iterator changeMask(iterator ace, uint32_t mask);
	void moveEveryoneAcesDown();
	void propagateEveryone();
	void propagateEveryone(const Ace &who, uint32_t allow);
	void setOwnerPermissions();
	void setOtherPermissions(uint32_t &added);
	uint32_t getMaxAllowed();
	void isolateOwnerClass();
	void isolateWho(const Ace &who, uint32_t deny);
	void isolateGroupClass(uint32_t deny);
	void applyMasks2AceList(uint32_t owner);
	void computeMaxMasks();

	uint32_t allowedToWho(const Ace &who) const;
	uint32_t groupClassAllowed();
	bool hasGroupEntry() const;

	static RichACL inherit(const RichACL &dir_acl, bool is_dir);

	uint32_t owner_mask_;
	uint32_t group_mask_;
	uint32_t other_mask_;
	uint16_t flags_;
	AceList ace_list_;
};

template <class GroupsContainer>
bool RichACL::checkPermission(uint32_t requested, uint32_t owner_uid, uint32_t owner_gid,
		uint32_t uid, const GroupsContainer &group_container) {
	auto it = std::find(group_container.begin(), group_container.end(), owner_gid);
	bool in_owning_group = it != group_container.end();
	bool in_owner_or_group_class = in_owning_group;
	uint32_t denied = 0;
	uint32_t mask = requested;

	if (flags_ & MASKED) {
		if (flags_ & WRITE_THROUGH && owner_uid == uid) {
			return !(requested & ~owner_mask_);
		}
	} else {
		in_owner_or_group_class = true;
	}

	for (const auto &ace : ace_list_) {
		uint32_t ace_mask = ace.mask;

		if (ace.isInheritOnly()) {
			continue;
		}

		if (ace.isOwner()) {
			if (owner_uid != uid) {
				continue;
			}
			goto entry_matches_owner;
		} else if (ace.isGroup()) {
			if (!in_owning_group) {
				continue;
			}
		} else if (ace.isUnixUser()) {
			if (uid != ace.id) {
				continue;
			}
			if (uid == owner_uid) {
				goto entry_matches_owner;
			}
		} else if (ace.isUnixGroup()) {
			auto it = std::find(group_container.begin(), group_container.end(), ace.id);
			if (it == group_container.end()) {
				continue;
			}
		} else {
			goto entry_matches_everyone;
		}

		if ((flags_ & MASKED) && ace.isAllow()) {
			ace_mask &= group_mask_;
		}

	entry_matches_owner:
		in_owner_or_group_class = true;

	entry_matches_everyone:
		if (ace.isDeny()) {
			denied |= ace_mask & mask;
		}
		mask &= ~ace_mask;

		if (!mask && in_owner_or_group_class) {
			break;
		}
	}
	denied |= mask;

	if (flags_ & MASKED) {
		if (uid == owner_uid) {
			denied |= requested & ~owner_mask_;
		} else if (in_owner_or_group_class) {
			denied |= requested & ~group_mask_;
		} else {
			if (flags_ & WRITE_THROUGH) {
				denied = requested & ~other_mask_;
			} else {
				denied |= requested & ~other_mask_;
			}
		}
	}

	return !denied;
}

inline uint32_t RichACL::convertMode2Mask(uint16_t mode) {
	uint32_t mask = 0;

	if (mode & S_IROTH) {
		mask |= Ace::POSIX_MODE_READ;
	}
	if (mode & S_IWOTH) {
		mask |= Ace::POSIX_MODE_WRITE;
	}
	if (mode & S_IXOTH) {
		mask |= Ace::POSIX_MODE_EXEC;
	}

	return mask;
}

inline uint16_t RichACL::convertMask2Mode(uint32_t mask) {
	uint16_t mode = 0;

	if (mask & Ace::POSIX_MODE_READ) {
		mode |= S_IROTH;
	}
	if (mask & Ace::POSIX_MODE_WRITE) {
		mode |= S_IWOTH;
	}
	if (mask & Ace::POSIX_MODE_EXEC) {
		mode |= S_IXOTH;
	}

	return mode;
}
