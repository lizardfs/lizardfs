/*
   Copyright 2015 Skytechnology sp. z o.o.

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

#include <cassert>

#include "common/chunk_part_type.h"
#include "common/goal.h"

namespace slice_traits {

inline bool isStandard(Goal::Slice::Type type) {
	return (int)type == Goal::Slice::Type::kStandard;
}

inline bool isStandard(const Goal::Slice &slice) {
	return isStandard(slice.getType());
}

inline bool isStandard(const ::ChunkPartType &cpt) {
	return isStandard(cpt.getSliceType());
}

inline bool isTape(Goal::Slice::Type type) {
	return (int)type == Goal::Slice::Type::kTape;
}

inline bool isTape(const Goal::Slice &slice) {
	return isTape(slice.getType());
}

inline bool isTape(const ::ChunkPartType &cpt) {
	return isTape(cpt.getSliceType());
}

inline bool isXor(Goal::Slice::Type type) {
	int value = (int)type;
	return value >= Goal::Slice::Type::kXor2 && value <= Goal::Slice::Type::kXor9;
}

inline bool isXor(const ::ChunkPartType &cpt) {
	return isXor(cpt.getSliceType());
}

inline bool isXor(const ::Goal::Slice &slice) {
	return isXor(slice.getType());
}

namespace standard {

inline ::ChunkPartType ChunkPartType() {
	return ::ChunkPartType(Goal::Slice::Type(Goal::Slice::Type::kStandard), 0);
}

} // standard

namespace tape {

inline ::ChunkPartType ChunkPartType() {
	return ::ChunkPartType(Goal::Slice::Type(Goal::Slice::Type::kTape), 0);
}

} // tape

namespace xors {

constexpr int kXorParityPart = 0;
constexpr int kMinXorLevel   = 2;
constexpr int kMaxXorLevel   = 9;

inline ::ChunkPartType ChunkPartType(int level, int part) {
	assert(level >= kMinXorLevel && level <= kMaxXorLevel);
	assert(part <= level);
	return ::ChunkPartType(Goal::Slice::Type((level - kMinXorLevel) + Goal::Slice::Type::kXor2),
	                       part);
}

inline bool isXorParity(const ::ChunkPartType &cpt) {
	return ::slice_traits::isXor(cpt) && cpt.getSlicePart() == kXorParityPart;
}

inline int getXorLevel(Goal::Slice::Type type) {
	assert(::slice_traits::isXor(type));
	return (int)type - (int)Goal::Slice::Type::kXor2 + kMinXorLevel;
}

inline int getXorLevel(const ::ChunkPartType &cpt) {
	return (int)cpt.getSliceType() - Goal::Slice::Type::kXor2 + kMinXorLevel;
}

inline int getXorLevel(const ::Goal::Slice &slice) {
	return getXorLevel(slice.getType());
}

inline int getXorPart(const ::ChunkPartType &cpt) {
	return cpt.getSlicePart();
}

inline bool isXorLevelValid(int level) {
	return level >= kMinXorLevel && level <= kMaxXorLevel;
}

} // xors

inline int getStripeSize(const ::ChunkPartType &cpt) {
	return isXor(cpt) ? xors::getXorLevel(cpt) : 1;
}

// Returns number of blocks of chunk that are stored in this
// part if the chunk has blockInChunk blocks
inline uint32_t getNumberOfBlocks(const ::ChunkPartType &cpt, uint32_t block_in_chunk) {
	if (isStandard(cpt)) {
		return block_in_chunk;
	} else {
		assert(isXor(cpt));
		uint32_t position_in_stripe =
		        (xors::isXorParity(cpt) ? xors::getXorLevel(cpt) - 1
		                                : xors::getXorLevel(cpt) - xors::getXorPart(cpt));
		return (block_in_chunk + position_in_stripe) / xors::getXorLevel(cpt);
	}
}

inline uint32_t chunkLengthToChunkPartLength(const ChunkPartType &cpt, uint32_t chunk_length) {
	if (isStandard(cpt)) {
		return chunk_length;
	}
	assert(isXor(cpt));

	uint32_t full_stripe = chunk_length / (xors::getXorLevel(cpt) * MFSBLOCKSIZE);
	uint32_t base_len = full_stripe * MFSBLOCKSIZE;
	uint32_t base = base_len * xors::getXorLevel(cpt);
	uint32_t rest = chunk_length - base;

	uint32_t tmp = 0;
	if (!xors::isXorParity(cpt)) {
		tmp = xors::getXorPart(cpt) - 1;
	}

	int32_t rest_len = rest - tmp * MFSBLOCKSIZE;
	if (rest_len < 0) {
		rest_len = 0;
	} else if (rest_len > MFSBLOCKSIZE) {
		rest_len = MFSBLOCKSIZE;
	}

	return base_len + rest_len;
}

} // slice_traits
