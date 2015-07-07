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

#pragma once

#include "common/platform.h"

#include "common/serialization_macros.h"
#include "common/media_label.h"

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(ChunkserverListEntry,
		uint32_t, version,
		uint32_t, servip,
		uint16_t, servport,
		uint64_t, usedspace,
		uint64_t, totalspace,
		uint32_t, chunkscount,
		uint64_t, todelusedspace,
		uint64_t, todeltotalspace,
		uint32_t, todelchunkscount,
		uint32_t, errorcounter,
		MediaLabel, label);
