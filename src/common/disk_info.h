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

#include <string>

#include "common/moosefs_string.h"
#include "common/serialization_macros.h"

SERIALIZABLE_CLASS_BEGIN(HddStatistics)
SERIALIZABLE_CLASS_BODY(HddStatistics,
		uint64_t, rbytes,
		uint64_t, wbytes,
		uint64_t, usecreadsum,
		uint64_t, usecwritesum,
		uint64_t, usecfsyncsum,
		uint32_t, rops,
		uint32_t, wops,
		uint32_t, fsyncops,
		uint32_t, usecreadmax,
		uint32_t, usecwritemax,
		uint32_t, usecfsyncmax)

	void clear() {
		*this = HddStatistics();
	}
	void add(const HddStatistics& other);
SERIALIZABLE_CLASS_END;

SERIALIZABLE_CLASS_BEGIN(DiskInfo)
SERIALIZABLE_CLASS_BODY(DiskInfo,
		uint16_t, entrySize,
		MooseFsString<uint8_t>, path,
		uint8_t, flags,
		uint64_t, errorChunkId,
		uint32_t, errorTimeStamp,
		uint64_t, used,
		uint64_t, total,
		uint32_t, chunksCount,
		HddStatistics, lastMinuteStats,
		HddStatistics, lastHourStats,
		HddStatistics, lastDayStats)

	static const uint32_t kToDeleteFlagMask = 0x1;
	static const uint32_t kDamagedFlagMask = 0x2;
	static const uint32_t kScanInProgressFlagMask = 0x4;
SERIALIZABLE_CLASS_END;
