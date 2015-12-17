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

inline Goal::Slice::Type getSliceType(int level) {
	assert(level >= kMinXorLevel && level <= kMaxXorLevel);
	return Goal::Slice::Type((level - kMinXorLevel) + Goal::Slice::Type::kXor2);
}

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

inline bool isParityPart(const ::ChunkPartType &cpt) {
	if (isXor(cpt)) {
		return xors::isXorParity(cpt);
	}
	return false;
}

inline bool isDataPart(const ::ChunkPartType &cpt) {
	return !isParityPart(cpt);
}

inline int getNumberOfDataParts(const ::Goal::Slice::Type &type) {
	if (isXor(type)) {
		return xors::getXorLevel(type);
	}
	return 1;
}

inline int getNumberOfDataParts(const ::ChunkPartType &cpt) {
	return getNumberOfDataParts(cpt.getSliceType());
}

inline int getNumberOfDataParts(const Goal::Slice &slice) {
	return getNumberOfDataParts(slice.getType());
}

inline int getNumberOfParityParts(const ::Goal::Slice::Type &type) {
	if (isXor(type)) {
		return 1;
	}
	return 0;
}

inline int getNumberOfParityParts(const ::ChunkPartType &cpt) {
	return getNumberOfParityParts(cpt.getSliceType());
}

inline int getNumberOfParityParts(const Goal::Slice &slice) {
	return getNumberOfParityParts(slice.getType());
}

inline int getNumberOfParts(const Goal::Slice::Type &type) {
	return type.expectedParts();
}

inline int getNumberOfParts(const Goal::Slice &slice) {
	return slice.size();
}

inline int getNumberOfParts(const ::ChunkPartType &cpt) {
	return cpt.getSliceType().expectedParts();
}

inline int requiredPartsToRecover(const ::Goal::Slice::Type &type) {
	return getNumberOfDataParts(type);
}

inline int requiredPartsToRecover(const ::ChunkPartType &cpt) {
	return requiredPartsToRecover(cpt.getSliceType());
}

inline int getDataPartIndex(const ::ChunkPartType &cpt) {
	if (isXor(cpt)) {
		return cpt.getSlicePart() - 1;
	}
	return 0;
}

inline int getParityPartIndex(const ::ChunkPartType &) {
	return 0;
}

inline int getStripeSize(const ::ChunkPartType &cpt) {
	return getNumberOfDataParts(cpt);
}

/*!
 * \brief Calculates number of blocks stored in chunk part.
 *
 * This function calculates the number of blocks stored in given chunk part
 * based on total number of blocks in chunk.
 *
 * \param cpt - chunk part for which calculations are done
 * \param blocks_in_chunk - total blocks count in chunk
 * \return number of blocks stored in given part
 */
inline int getNumberOfBlocks(const ::ChunkPartType &cpt, uint32_t blocks_in_chunk) {
	int data_part_index = isDataPart(cpt) ? getDataPartIndex(cpt) : 0;

	return (blocks_in_chunk + (getNumberOfDataParts(cpt) - data_part_index - 1)) /
	       getNumberOfDataParts(cpt);
}

inline int getNumberOfBlocks(const ::ChunkPartType &cpt) {
	return getNumberOfBlocks(cpt, MFSBLOCKSINCHUNK);
}

/*!
 * \brief Calculates byte length of chunk part.
 *
 * This function calculate byte length of given chunk part
 * based on total length of chunk.
 *
 * \param cpt - chunk part type for which calculations are done
 * \param chunk_length - total byte length of chunk
 * \return byte length of chunk part
 */
inline int chunkLengthToChunkPartLength(const ChunkPartType &cpt, int chunk_length) {
	// 'if' is only needed to make it easier for compiler to optimize out this function
	if(getNumberOfDataParts(cpt) == 1) {
		return chunk_length;
	}

	int full_stripe = chunk_length / (getNumberOfDataParts(cpt) * MFSBLOCKSIZE);
	int base_len = full_stripe * MFSBLOCKSIZE;
	int base = base_len * getNumberOfDataParts(cpt);
	int rest = chunk_length - base;

	int data_part_index = isDataPart(cpt) ? getDataPartIndex(cpt) : 0;

	int part_rest_len = std::max(rest - data_part_index * MFSBLOCKSIZE, 0);
	part_rest_len = std::min(part_rest_len, MFSBLOCKSIZE);

	return base_len + part_rest_len;
}

} // slice_traits
