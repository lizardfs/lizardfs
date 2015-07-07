/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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
#include "common/extended_acl.h"

#include <tuple>

bool ExtendedAcl::Entry::operator==(const ExtendedAcl::Entry& other) const {
	return std::make_tuple(type, id, mask) == std::make_tuple(other.type, other.id, other.mask);
}

uint32_t ExtendedAcl::Entry::serializedSize() const {
	return ::serializedSize(bool(), id, mask);
}

void ExtendedAcl::Entry::serialize(uint8_t** destination) const {
	::serialize(destination, (type == EntryType::kNamedUser), id, mask);
}

void ExtendedAcl::Entry::deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer) {
	bool isNamedUser;
	::deserialize(source, bytesLeftInBuffer, isNamedUser, id, mask);
	type = (isNamedUser ? EntryType::kNamedUser : EntryType::kNamedGroup);
}
