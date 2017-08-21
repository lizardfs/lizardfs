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

bool RichACL::isSameMode(uint16_t mode, bool is_dir) const {
	uint32_t x = is_dir ? 0 : Ace::DELETE_CHILD;
	uint32_t owner_mask, group_mask, other_mask;

	owner_mask = convertMode2Mask(mode >> 6) & ~x;
	group_mask = convertMode2Mask(mode >> 3) & ~x;
	other_mask = convertMode2Mask(mode) & ~x;

	return owner_mask == owner_mask_ && group_mask == group_mask_ && other_mask == other_mask_ &&
	       (flags_ & MASKED) && (flags_ & WRITE_THROUGH) && (!isAutoInherit() || !isProtected());
}

void RichACL::setMode(uint16_t mode, bool is_dir) {
	uint32_t x = is_dir ? 0 : Ace::DELETE_CHILD;

	flags_ |= MASKED | WRITE_THROUGH;
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
		owner_mask &= ~Ace::DELETE_CHILD;
		group_mask &= ~Ace::DELETE_CHILD;
		other_mask &= ~Ace::DELETE_CHILD;
	}

	acl.owner_mask_ = owner_mask;
	acl.group_mask_ = group_mask;
	acl.other_mask_ = other_mask;

	denied = ~owner_mask & (group_mask | other_mask);
	if (denied) {
		acl.insert(
		    Ace(Ace::ACCESS_DENIED_ACE_TYPE, Ace::SPECIAL_WHO, denied, Ace::OWNER_SPECIAL_ID));
	}
	if (owner_mask & ~(group_mask & other_mask)) {
		acl.insert(
		    Ace(Ace::ACCESS_ALLOWED_ACE_TYPE, Ace::SPECIAL_WHO, owner_mask, Ace::OWNER_SPECIAL_ID));
	}
	denied = ~group_mask & other_mask;
	if (denied) {
		acl.insert(
		    Ace(Ace::ACCESS_DENIED_ACE_TYPE, Ace::SPECIAL_WHO, denied, Ace::GROUP_SPECIAL_ID));
	}
	if (group_mask & ~other_mask) {
		acl.insert(
		    Ace(Ace::ACCESS_ALLOWED_ACE_TYPE, Ace::SPECIAL_WHO, group_mask, Ace::GROUP_SPECIAL_ID));
	}
	if (other_mask) {
		acl.insert(Ace(Ace::ACCESS_ALLOWED_ACE_TYPE, Ace::SPECIAL_WHO, other_mask,
		               Ace::EVERYONE_SPECIAL_ID));
	}

	return acl;
}

uint32_t RichACL::allowedToWho(const Ace &who) {
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

	flags_ &= ~(WRITE_THROUGH | MASKED);
}
