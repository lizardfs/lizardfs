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

RichACL::iterator RichACL::changeMask(iterator ace, uint32_t mask) {
	if (mask && ace->mask == mask) {
		ace->flags &= ~Ace::kInheritOnlyAce;
	} else if (mask & ~Ace::kPosixAlwaysAllowed) {
		if (ace->isInheritable()) {
			ace = ace_list_.insert(ace, *ace);
			ace->flags |= Ace::kInheritOnlyAce;
			++ace;
			ace->flags &= ~Ace::kInheritanceFlags | Ace::kInheritedAce;
		}
		ace->mask = mask;
	} else {
		if (ace->isInheritable()) {
			ace->flags |= Ace::kInheritOnlyAce;
		} else {
			return ace_list_.erase(ace);
		}
	}
	++ace;
	return ace;
}

void RichACL::moveEveryoneAcesDown() {
	iterator ace;
	uint32_t allowed = 0, denied = 0;

	for (ace = ace_list_.begin(); ace != ace_list_.end();) {
		if (ace->isInheritOnly() || !(ace->isAllow() || ace->isDeny())) {
			++ace;
			continue;
		}
		if (ace->isEveryone()) {
			if (ace->isAllow()) {
				allowed |= ace->mask & ~denied;
			} else {
				assert(ace->isDeny());
				denied |= ace->mask & ~allowed;
			}
			ace = changeMask(ace, 0);
			continue;
		}

		if (ace->isAllow()) {
			ace = changeMask(ace, allowed | (ace->mask & ~denied));
		} else {
			assert(ace->isDeny());
			ace = changeMask(ace, denied | (ace->mask & ~allowed));
		}
	}

	if (allowed & ~Ace::kPosixAlwaysAllowed) {
		if (!ace_list_.empty()) {
			Ace &last_ace(ace_list_.back());
			if (last_ace.isEveryone() && last_ace.isAllow() && last_ace.isInheritOnly() &&
			    last_ace.mask == allowed) {
				last_ace.mask &= ~Ace::kInheritOnlyAce;
				return;
			}
		}
		ace_list_.push_back(
		    Ace(Ace::kAccessAllowedAceType, Ace::kSpecialWho, allowed, Ace::kEveryoneSpecialId));
	}
}

void RichACL::propagateEveryone(const Ace &who, uint32_t allow) {
	iterator ace, allow_last;

	allow_last = ace_list_.end();
	for (ace = ace_list_.begin(); ace != ace_list_.end(); ++ace) {
		if (ace->isInheritOnly()) {
			continue;
		}

		if (ace->isAllow()) {
			if (ace->isSameIdentifier(who)) {
				allow &= ~ace->mask;
				allow_last = ace;
			}
		} else if (ace->isDeny()) {
			if (ace->isSameIdentifier(who)) {
				allow &= ~ace->mask;
			} else if (allow & ace->mask) {
				allow_last = ace_list_.end();
			}
		}
	}

	assert(!ace_list_.empty());
	ace = std::next(ace_list_.begin(), ace_list_.size() - 1);
	if (!ace->isOwner() && ace->isEveryone() && !(allow & ~(ace->mask & other_mask_))) {
		allow = 0;
	}

	if (allow) {
		if (allow_last != ace_list_.end()) {
			changeMask(allow_last, allow_last->mask | allow);
			return;
		}

		ace_list_.insert(ace, Ace(Ace::kAccessAllowedAceType, who.flags & ~Ace::kInheritanceFlags,
		                          allow, who.id));
	}
}

void RichACL::propagateEveryone() {
	if (ace_list_.empty()) {
		return;
	}

	auto ace = std::next(ace_list_.begin(), ace_list_.size() - 1);

	if (ace->isInheritOnly() || !ace->isEveryone()) {
		return;
	}

	Ace who(0, Ace::kSpecialWho, 0, 0);
	uint32_t owner_allow = ace->mask & owner_mask_;
	uint32_t group_allow = ace->mask & group_mask_;

	if (owner_allow & ~(group_mask_ & other_mask_)) {
		who.id = Ace::kOwnerSpecialId;
		propagateEveryone(who, owner_allow);
	}

	if (group_allow & ~other_mask_) {
		who.id = Ace::kGroupSpecialId;
		propagateEveryone(who, group_allow);

		for (int i = (int)ace_list_.size() - 2; i >= 0; --i) {
			ace = std::next(ace_list_.begin(), i);
			if (ace->isInheritOnly() || ace->isOwner() || ace->isGroup()) {
				continue;
			}
			propagateEveryone(*ace, group_allow);
		}
	}
}

void RichACL::setOwnerPermissions() {
	uint32_t owner_mask = owner_mask_ & ~Ace::kPosixAlwaysAllowed;
	uint32_t denied = 0;

	if (!(flags_ & kWriteThrough)) {
		return;
	}

	for (iterator ace = ace_list_.begin(); ace != ace_list_.end();) {
		if (ace->isOwner()) {
			if (ace->isAllow() && !(owner_mask & denied)) {
				ace = changeMask(ace, owner_mask);
				owner_mask = 0;
			} else {
				ace = changeMask(ace, 0);
			}
		} else {
			if (ace->isDeny()) {
				denied |= ace->mask;
			}
			++ace;
		}
	}

	if (owner_mask & (denied | ~other_mask_ | ~group_mask_)) {
		ace_list_.insert(ace_list_.begin(), Ace(Ace::kAccessAllowedAceType, Ace::kSpecialWho,
		                                        owner_mask, Ace::kOwnerSpecialId));
	}
}

void RichACL::setOtherPermissions(uint32_t &added) {
	uint32_t other_mask = other_mask_ & ~Ace::kPosixAlwaysAllowed;

	if (!(other_mask && (flags_ & kWriteThrough))) {
		return;
	}

	added = other_mask;

	if (ace_list_.empty() || !ace_list_.back().isEveryone() || ace_list_.back().isInheritOnly()) {
		ace_list_.push_back(Ace(Ace::kAccessAllowedAceType, Ace::kSpecialWho, other_mask,
		                        Ace::kEveryoneSpecialId));
	} else {
		iterator ace = ace_list_.begin() + (ace_list_.size() - 1);
		added &= ~ace->mask;
		changeMask(ace, other_mask);
	}
}

uint32_t RichACL::getMaxAllowed() {
	uint32_t allowed = 0;

	for (auto ace = ace_list_.crbegin(); ace != ace_list_.crend(); ++ace) {
		if (ace->isInheritOnly()) {
			continue;
		}
		if (ace->isAllow()) {
			allowed |= ace->mask;
		} else if (ace->isDeny() && ace->isEveryone()) {
			allowed &= ~ace->mask;
		}
	}
	return allowed;
}

void RichACL::isolateOwnerClass() {
	uint32_t deny = getMaxAllowed() & ~owner_mask_;

	if (deny == 0) {
		return;
	}

	for (iterator ace = ace_list_.begin(); ace != ace_list_.end(); ++ace) {
		if (ace->isInheritOnly()) {
			continue;
		}
		if (ace->isAllow()) {
			break;
		}
		if (ace->isOwner()) {
			changeMask(ace, ace->mask | deny);
			return;
		}
	}

	ace_list_.insert(ace_list_.begin(), Ace(Ace::kAccessDeniedAceType, Ace::kSpecialWho, deny,
	                                        Ace::kOwnerSpecialId));
}

void RichACL::isolateWho(const Ace &who, uint32_t deny) {
	for (const auto &ace : ace_list_) {
		if (ace.isInheritOnly()) {
			continue;
		}
		if (ace.isSameIdentifier(who)) {
			deny &= ~ace.mask;
		}
	}

	if (deny == 0) {
		return;
	}

	for (int i = (int)ace_list_.size() - 2; i >= 0; --i) {
		auto ace = std::next(ace_list_.begin(), i);
		if (ace->isInheritOnly()) {
			continue;
		}
		if (ace->isDeny()) {
			if (ace->isSameIdentifier(who)) {
				changeMask(ace, ace->mask | deny);
				return;
			}
		} else if (ace->isAllow() && (ace->mask & deny)) {
			break;
		}
	}

	auto ace = ace_list_.begin() + (ace_list_.size() - 1);
	ace_list_.insert(
	    ace, Ace(Ace::kAccessDeniedAceType, who.flags & ~Ace::kInheritanceFlags, deny, who.id));
}

void RichACL::isolateGroupClass(uint32_t deny) {
	Ace who(0, Ace::kSpecialWho, 0, Ace::kGroupSpecialId);

	if (ace_list_.empty()) {
		return;
	}

	auto ace = std::next(ace_list_.begin(), ace_list_.size() - 1);
	if (ace->isInheritOnly() || !ace->isEveryone()) {
		return;
	}

	deny |= ace->mask & ~group_mask_;
	if (deny == 0) {
		return;
	}

	isolateWho(who, deny);

	for (int i = (int)ace_list_.size() - 2; i >= 0; --i) {
		ace = std::next(ace_list_.begin(), i);

		if (ace->isInheritOnly() || ace->isOwner() || ace->isGroup()) {
			continue;
		}
		isolateWho(*ace, deny);
	}
}

void RichACL::applyMasks2AceList(uint32_t owner) {
	for (auto ace = ace_list_.begin(); ace != ace_list_.end();) {
		if (ace->isInheritOnly() || !ace->isAllow()) {
			++ace;
			continue;
		}

		uint32_t mask = 0;
		if (ace->isOwner() || (ace->isUnixUser() && owner == ace->id)) {
			mask = owner_mask_;
		} else if (ace->isEveryone()) {
			mask = other_mask_;
		} else {
			mask = group_mask_;
		}

		ace = changeMask(ace, ace->mask & mask);
	}
}

void RichACL::applyMasks(uint32_t owner) {
	if (!(flags_ & kMasked)) {
		return;
	}

	uint32_t added = 0;

	moveEveryoneAcesDown();
	propagateEveryone();
	applyMasks2AceList(owner);
	setOtherPermissions(added);
	isolateGroupClass(added);
	setOwnerPermissions();
	isolateOwnerClass();

	flags_ &= ~(kWriteThrough | kMasked);

	ace_list_.shrink_to_fit();
}
