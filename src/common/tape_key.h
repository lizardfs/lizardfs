#pragma once

#include "common/platform.h"

#include "common/serialization_macros.h"

LIZARDFS_DEFINE_SERIALIZABLE_CLASS(TapeKey,
		uint32_t, inode,
		uint32_t, mtime,
		uint64_t, fileLength);
