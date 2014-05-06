#pragma once

#include "config.h"

#include "common/serialization_macros.h"

LIZARDFS_DEFINE_SERIALIZABLE_ENUM_CLASS(QuotaRigor, kSoft, kHard);
LIZARDFS_DEFINE_SERIALIZABLE_ENUM_CLASS(QuotaResource, kInodes, kSize);
LIZARDFS_DEFINE_SERIALIZABLE_ENUM_CLASS(QuotaOwner, kUser, kGroup);
