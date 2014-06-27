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
