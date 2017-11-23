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

#include "common/platform.h"
#include "common/richacl.h"

#include <cassert>
#include <numeric>

bool RichACL::isSameMode(uint16_t mode, bool is_dir) const {
	uint32_t x = is_dir ? 0 : Ace::kDeleteChild;
	uint32_t owner_mask, group_mask, other_mask;

	owner_mask = convertMode2Mask(mode >> 6) & ~x;
	group_mask = convertMode2Mask(mode >> 3) & ~x;
	other_mask = convertMode2Mask(mode) & ~x;

	return owner_mask == owner_mask_ && group_mask == group_mask_ && other_mask == other_mask_ &&
	       (flags_ & kMasked) && (flags_ & kWriteThrough) && (!isAutoInherit() || !isProtected());
}

void RichACL::setMode(uint16_t mode, bool is_dir) {
	uint32_t x = is_dir ? 0 : Ace::kDeleteChild;

	flags_ |= kMasked | kWriteThrough;
	owner_mask_ = convertMode2Mask(mode >> 6) & ~x;
	group_mask_ = convertMode2Mask(mode >> 3) & ~x;
	other_mask_ = convertMode2Mask(mode) & ~x;
}

uint16_t RichACL::getMode() const {
	return convertMask2Mode(owner_mask_) << 6 | convertMask2Mode(group_mask_) << 3 |
	       convertMask2Mode(other_mask_);
}

RichACL RichACL::createFromMode(uint16_t mode, bool is_dir) {
	uint32_t owner_mask = convertMode2Mask(mode >> 6);
	uint32_t group_mask = convertMode2Mask(mode >> 3);
	uint32_t other_mask = convertMode2Mask(mode);
	uint32_t denied;
	RichACL acl;

	if (!is_dir) {
		owner_mask &= ~Ace::kDeleteChild;
		group_mask &= ~Ace::kDeleteChild;
		other_mask &= ~Ace::kDeleteChild;
	}

	acl.owner_mask_ = owner_mask;
	acl.group_mask_ = group_mask;
	acl.other_mask_ = other_mask;

	denied = ~owner_mask & (group_mask | other_mask);
	if (denied) {
		acl.insert(
		    Ace(Ace::kAccessDeniedAceType, Ace::kSpecialWho, denied, Ace::kOwnerSpecialId));
	}
	if (owner_mask & ~(group_mask & other_mask)) {
		acl.insert(
		    Ace(Ace::kAccessAllowedAceType, Ace::kSpecialWho, owner_mask, Ace::kOwnerSpecialId));
	}
	denied = ~group_mask & other_mask;
	if (denied) {
		acl.insert(
		    Ace(Ace::kAccessDeniedAceType, Ace::kSpecialWho, denied, Ace::kGroupSpecialId));
	}
	if (group_mask & ~other_mask) {
		acl.insert(
		    Ace(Ace::kAccessAllowedAceType, Ace::kSpecialWho, group_mask, Ace::kGroupSpecialId));
	}
	if (other_mask) {
		acl.insert(Ace(Ace::kAccessAllowedAceType, Ace::kSpecialWho, other_mask,
		               Ace::kEveryoneSpecialId));
	}

	return acl;
}

uint32_t RichACL::allowedToWho(const Ace &who) const {
	uint32_t allowed = 0;

	for (auto ace = ace_list_.crbegin(); ace != ace_list_.crend(); ++ace) {
		if (ace->isInheritOnly()) {
			continue;
		}
		if (ace->isSameIdentifier(who) || ace->isEveryone()) {
			if (ace->isAllow()) {
				allowed |= ace->mask;
			} else if (ace->isDeny()) {
				allowed &= ~ace->mask;
			}
		}
	}

	return allowed;
}

uint32_t RichACL::groupClassAllowed() {
	uint32_t everyone_allowed = 0, group_class_allowed = 0;
	bool had_group_ace = false;

	for (auto ace = ace_list_.crbegin(); ace != ace_list_.crend(); ++ace) {
		if (ace->isInheritOnly() || ace->isOwner()) {
			continue;
		}

		if (ace->isEveryone()) {
			if (ace->isAllow()) {
				everyone_allowed |= ace->mask;
			} else if (ace->isDeny()) {
				everyone_allowed &= ~ace->mask;
			}
		} else {
			group_class_allowed |= allowedToWho(*ace);

			if (ace->isGroup()) {
				had_group_ace = true;
			}
		}
	}
	if (!had_group_ace) {
		group_class_allowed |= everyone_allowed;
	}

	return group_class_allowed;
}

void RichACL::computeMaxMasks() {
	owner_mask_ = 0;
	group_mask_ = 0;
	other_mask_ = 0;

	uint32_t gmask = ~(uint32_t)0;
	for (const auto &ace : ace_list_) {
		if (ace.isInheritOnly()) {
			continue;
		}

		if (!ace.isOwner() && !ace.isEveryone() && ace.isDeny()) {
			gmask = groupClassAllowed();
			break;
		}
	}

	for (auto ace = ace_list_.crbegin(); ace != ace_list_.crend(); ++ace) {
		if (ace->isInheritOnly()) {
			continue;
		}

		if (ace->isOwner()) {
			if (ace->isAllow()) {
				owner_mask_ |= ace->mask;
			} else if (ace->isDeny()) {
				owner_mask_ &= ~ace->mask;
			}
		} else if (ace->isEveryone()) {
			if (ace->isAllow()) {
				owner_mask_ |= ace->mask;
				group_mask_ |= ace->mask & gmask;
				other_mask_ |= ace->mask;
			} else if (ace->isDeny()) {
				owner_mask_ &= ~ace->mask;
				group_mask_ &= ~ace->mask;
				other_mask_ &= ~ace->mask;
			}
		} else {
			if (ace->isAllow()) {
				owner_mask_ |= ace->mask & gmask;
				group_mask_ |= ace->mask & gmask;
			}
		}
	}

	flags_ &= ~(kWriteThrough | kMasked);
}

void RichACL::removeInheritOnly(bool remove_with_flag_set) {
	auto it =
	    std::remove_if(ace_list_.begin(), ace_list_.end(), [remove_with_flag_set](const Ace &ace) {
		    return (remove_with_flag_set && ace.isInheritOnly()) ||
		           (!remove_with_flag_set && !ace.isInheritOnly());
		});
	ace_list_.erase(it, ace_list_.end());
}

bool RichACL::checkInheritFlags(bool is_directory) const {
	for (const auto &ace : ace_list_) {
		if (ace.isInheritOnly() && !ace.isInheritable()) {
			return false;
		}
	}

	if (is_directory) {
		return true;
	}

	for (const auto &ace : ace_list_) {
		if (ace.isInheritOnly() || ace.isInheritable()) {
			return false;
		}
	}

	return true;
}

RichACL RichACL::inherit(const RichACL &dir_acl, bool is_dir) {
	RichACL acl;

	if (is_dir) {
		int count = std::accumulate(dir_acl.begin(), dir_acl.end(), 0,
		                            [](int sum, const RichACL::Ace &dir_ace) {
			                            return sum + (int)dir_ace.inheritsToDirectory();
			                        });
		acl.ace_list_.reserve(count);
		for (const auto &dir_ace : dir_acl) {
			if (!dir_ace.inheritsToDirectory()) {
				continue;
			}

			Ace ace = dir_ace;
			if (dir_ace.flags & Ace::kNoPropagateInheritAce) {
				ace.flags &= ~Ace::kInheritanceFlags;
			} else if (dir_ace.flags & Ace::kDirectoryInheritAce) {
				ace.flags &= ~Ace::kInheritOnlyAce;
			} else {
				ace.flags |= Ace::kInheritOnlyAce;
			}

			acl.insert(ace);
		}
	} else {
		int count = std::accumulate(dir_acl.begin(), dir_acl.end(), 0,
		                            [](int sum, const RichACL::Ace &dir_ace) {
			                            return sum + ((dir_ace.flags & Ace::kFileInheritAce) ? 1 : 0);
			                        });
		acl.ace_list_.reserve(count);
		for (const auto &dir_ace : dir_acl) {
			if (!(dir_ace.flags & Ace::kFileInheritAce)) {
				continue;
			}

			Ace ace = dir_ace;

			ace.flags &= ~Ace::kInheritanceFlags;
			ace.mask &= ~Ace::kDeleteChild;

			acl.insert(ace);
		}
	}

	if (dir_acl.isAutoInherit()) {
		acl.flags_ = kAutoInherit;
		for (auto &ace : acl) {
			ace.flags |= Ace::kInheritedAce;
		}
	} else {
		for (auto &ace : acl) {
			ace.flags &= ~Ace::kInheritedAce;
		}
	}

	return acl;
}

bool RichACL::equivMode(const RichACL &acl, uint16_t &mode_out, bool is_dir) {
	uint16_t mode = mode_out;

	uint32_t x = is_dir ? 0 : Ace::kDeleteChild;
	uint32_t owner_allowed = 0,
	         owner_defined = Ace::kPosixAlwaysAllowed | Ace::kPosixOwnerAllowed | x;
	uint32_t group_allowed = 0, group_defined = Ace::kPosixAlwaysAllowed | x;
	uint32_t everyone_allowed = 0, everyone_defined = Ace::kPosixAlwaysAllowed | x;

	if (acl.flags_ & ~(kWriteThrough | kMasked)) {
		return false;
	}
	if (acl.isAutoSetMode() && acl.ace_list_.empty()) {
		return true;
	}

	for (const auto &ace : acl) {
		if (ace.flags & ~Ace::kSpecialWho) {
			return false;
		}

		if (ace.isOwner() || ace.isEveryone()) {
			x = ace.mask & ~owner_defined;
			if (ace.isAllow()) {
				uint32_t group_denied = group_defined & ~group_allowed;
				if (x & group_denied) {
					return false;
				}
				owner_allowed |= x;
			} else {
				if (x & group_allowed) {
					return false;
				}
			}
			owner_defined |= x;

			if (ace.isEveryone()) {
				x = ace.mask;
				if (ace.isAllow()) {
					group_allowed |= x & ~group_defined;
					everyone_allowed |= x & ~everyone_defined;
				}
				group_defined |= x;
				everyone_defined |= x;
			}
		} else if (ace.isGroup()) {
			x = ace.mask & ~group_defined;
			if (ace.isAllow()) {
				group_allowed |= x;
			}
			group_defined |= x;
		} else {
			return false;
		}
	}

	if (group_allowed & ~owner_defined) {
		return false;
	}

	if (acl.flags_ & kMasked) {
		if (acl.flags_ & kWriteThrough) {
			owner_allowed = acl.owner_mask_;
			everyone_allowed = acl.other_mask_;
		} else {
			owner_allowed &= acl.owner_mask_;
			everyone_allowed &= acl.other_mask_;
		}
		group_allowed &= acl.group_mask_;
	}

	mode = (mode & ~0777) | (convertMask2Mode(owner_allowed) << 6) |
	       (convertMask2Mode(group_allowed) << 3) | convertMask2Mode(everyone_allowed);

	x = is_dir ? 0 : Ace::kDeleteChild;

	if (((convertMode2Mask(mode >> 6) ^ owner_allowed) & ~x) ||
	    ((convertMode2Mask(mode >> 3) ^ group_allowed) & ~x) ||
	    ((convertMode2Mask(mode) ^ everyone_allowed) & ~x)) {
		return false;
	}

	mode_out = mode;
	return true;
}

bool RichACL::inheritInode(const RichACL &dir_acl, uint16_t &mode_out, RichACL &acl, uint16_t umask,
		bool is_dir) {
	uint16_t mode = mode_out;

	acl = inherit(dir_acl, is_dir);
	if (acl.size() == 0) {
		mode_out &= ~umask;
		return false;
	}

	if (equivMode(acl, mode, is_dir)) {
		mode_out &= mode;
		return false;
	} else {
		if (acl.isAutoInherit()) {
			acl.flags_ |= kProtected;
		}

		acl.computeMaxMasks();

		acl.flags_ |= kMasked;
		acl.owner_mask_ &= convertMode2Mask(mode >> 6);
		acl.group_mask_ &= convertMode2Mask(mode >> 3);
		acl.other_mask_ &= convertMode2Mask(mode);

		mode_out = acl.getMode();
	}

	return true;
}

void RichACL::createExplicitInheritance() {
	int n = ace_list_.size();
	for (int i = 0; i < n; ++i) {
		Ace &ace = ace_list_[i];

		if (ace.isInheritOnly() || !ace.isInheritable()) {
			continue;
		}

		Ace ace_copy = ace;
		ace.flags &= ~(Ace::kInheritOnlyAce | Ace::kFileInheritAce | Ace::kDirectoryInheritAce);

		ace_copy.flags |= Ace::kInheritOnlyAce;

		ace_list_.push_back(ace_copy);
	}
}
