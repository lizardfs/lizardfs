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

LIZARDFS_DEFINE_SERIALIZABLE_ENUM_CLASS(QuotaRigor, kSoft, kHard)
LIZARDFS_DEFINE_SERIALIZABLE_ENUM_CLASS(QuotaResource, kInodes, kSize)
LIZARDFS_DEFINE_SERIALIZABLE_ENUM_CLASS(QuotaOwnerType, kUser, kGroup)

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(QuotaOwner,
		QuotaOwnerType, ownerType,
		uint32_t      , ownerId);

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(QuotaEntryKey,
		QuotaOwner   , owner,
		QuotaRigor   , rigor,
		QuotaResource, resource);

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(QuotaLimits,
		uint64_t, inodesSoftLimit,
		uint64_t, inodesHardLimit,
		uint64_t, inodes,
		uint64_t, bytesSoftLimit,
		uint64_t, bytesHardLimit,
		uint64_t, bytes);

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(QuotaOwnerAndLimits,
		QuotaOwner , owner,
		QuotaLimits, limits);

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(QuotaEntry,
		QuotaEntryKey, entryKey,
		uint64_t     , limit);
