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

#include "common/serialization_macros.h"
#include "common/attributes.h"

namespace legacy {
LIZARDFS_DEFINE_SERIALIZABLE_CLASS(DirectoryEntry,
	uint32_t, inode,
	std::string, name,
	Attributes, attributes);
} // namespace legacy

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(DirectoryEntry,
	uint64_t, index,
	uint64_t, next_index,
	uint32_t, inode,
	std::string, name,
	Attributes, attributes);
