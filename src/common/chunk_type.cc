/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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

#include "common/platform.h"
#include "common/chunk_type.h"

#include "common/goal.h"
#include "common/massert.h"
#include "common/slice_traits.h"

bool ChunkType::validChunkTypeID(uint8_t chunkTypeId) {
	if (chunkTypeId == ChunkType::kStandardChunkTypeId) {
		return true;
	}
	uint8_t xorLevel = chunkTypeId / (slice_traits::xors::kMaxXorLevel + 1);
	if (xorLevel < slice_traits::xors::kMinXorLevel || xorLevel > slice_traits::xors::kMaxXorLevel) {
		return false;
	}
	uint8_t xorPart = chunkTypeId % (slice_traits::xors::kMaxXorLevel + 1);
	if (xorPart == ChunkType::kXorParityPart) {
		return true;
	}
	if (xorPart < 1 || xorPart > xorLevel) {
		return false;
	}
	return true;
}

ChunkType ChunkType::getStandardChunkType() {
	return ChunkType(ChunkType::kStandardChunkTypeId);
}

ChunkType ChunkType::getXorChunkType(XorLevel level, XorPart part) {
	sassert(part <= level);
	sassert(level >= slice_traits::xors::kMinXorLevel);
	sassert(level <= slice_traits::xors::kMaxXorLevel);
	return ChunkType((slice_traits::xors::kMaxXorLevel + 1) * level + part);
}

ChunkType ChunkType::getXorParityChunkType(XorLevel level) {
	sassert(level >= slice_traits::xors::kMinXorLevel);
	sassert(level <= slice_traits::xors::kMaxXorLevel);
	return ChunkType((slice_traits::xors::kMaxXorLevel + 1) * level + ChunkType::kXorParityPart);
}

bool ChunkType::isStandardChunkType() const {
	return chunkTypeId_ == ChunkType::kStandardChunkTypeId;
}

bool ChunkType::isXorChunkType() const {
	return !isStandardChunkType();
}

bool ChunkType::isXorParity() const {
	sassert(isXorChunkType());
	return getXorPart() == ChunkType::kXorParityPart;
}

uint8_t ChunkType::chunkTypeId() const {
	return chunkTypeId_;
}

ChunkType::XorLevel ChunkType::getXorLevel() const {
	sassert(isXorChunkType());
	return chunkTypeId_ / (slice_traits::xors::kMaxXorLevel + 1);
}

ChunkType::XorPart ChunkType::getXorPart() const {
	sassert(isXorChunkType());
	return chunkTypeId_ % (slice_traits::xors::kMaxXorLevel + 1);
}
