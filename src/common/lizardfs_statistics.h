#pragma once

#include "common/serializable_class.h"

SERIALIZABLE_CLASS_BEGIN(LizardFsStatistics)
SERIALIZABLE_CLASS_BODY(LizardFsStatistics,
		uint32_t, version,
		uint64_t, memoryUsage,
		uint64_t, totalSpace,
		uint64_t, availableSpace,
		uint64_t, trashSpace,
		uint32_t, trashNodes,
		uint64_t, reservedSpace,
		uint32_t, reservedNodes,
		uint32_t, allNodes,
		uint32_t, dirNodes,
		uint32_t, fileNodes,
		uint32_t, chunks,
		uint32_t, chunkCopies,
		uint32_t, regularCopies)

		LizardFsStatistics() = default;
SERIALIZABLE_CLASS_END;
