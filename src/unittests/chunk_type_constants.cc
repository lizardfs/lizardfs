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
#include "unittests/chunk_type_constants.h"

const ChunkType standard{ChunkType::getStandardChunkType()};
const ChunkType xor_1_of_2{ChunkType::getXorChunkType(2, 1)};
const ChunkType xor_2_of_2{ChunkType::getXorChunkType(2, 2)};
const ChunkType xor_p_of_2{ChunkType::getXorParityChunkType(2)};
const ChunkType xor_1_of_3{ChunkType::getXorChunkType(3, 1)};
const ChunkType xor_2_of_3{ChunkType::getXorChunkType(3, 2)};
const ChunkType xor_3_of_3{ChunkType::getXorChunkType(3, 3)};
const ChunkType xor_p_of_3{ChunkType::getXorParityChunkType(3)};
const ChunkType xor_1_of_4{ChunkType::getXorChunkType(4, 1)};
const ChunkType xor_2_of_4{ChunkType::getXorChunkType(4, 2)};
const ChunkType xor_3_of_4{ChunkType::getXorChunkType(4, 3)};
const ChunkType xor_4_of_4{ChunkType::getXorChunkType(4, 4)};
const ChunkType xor_p_of_4{ChunkType::getXorParityChunkType(4)};
const ChunkType xor_1_of_6{ChunkType::getXorChunkType(6, 1)};
const ChunkType xor_2_of_6{ChunkType::getXorChunkType(6, 2)};
const ChunkType xor_3_of_6{ChunkType::getXorChunkType(6, 3)};
const ChunkType xor_4_of_6{ChunkType::getXorChunkType(6, 4)};
const ChunkType xor_5_of_6{ChunkType::getXorChunkType(6, 5)};
const ChunkType xor_6_of_6{ChunkType::getXorChunkType(6, 6)};
const ChunkType xor_p_of_6{ChunkType::getXorParityChunkType(6)};
const ChunkType xor_1_of_7{ChunkType::getXorChunkType(7, 1)};
const ChunkType xor_2_of_7{ChunkType::getXorChunkType(7, 2)};
const ChunkType xor_3_of_7{ChunkType::getXorChunkType(7, 3)};
const ChunkType xor_4_of_7{ChunkType::getXorChunkType(7, 4)};
const ChunkType xor_5_of_7{ChunkType::getXorChunkType(7, 5)};
const ChunkType xor_6_of_7{ChunkType::getXorChunkType(7, 6)};
const ChunkType xor_7_of_7{ChunkType::getXorChunkType(7, 7)};
const ChunkType xor_p_of_7{ChunkType::getXorParityChunkType(7)};
const ChunkType xor_1_of_9{ChunkType::getXorChunkType(9, 1)};
const ChunkType xor_2_of_9{ChunkType::getXorChunkType(9, 2)};
const ChunkType xor_3_of_9{ChunkType::getXorChunkType(9, 3)};
const ChunkType xor_4_of_9{ChunkType::getXorChunkType(9, 4)};
const ChunkType xor_5_of_9{ChunkType::getXorChunkType(9, 5)};
const ChunkType xor_6_of_9{ChunkType::getXorChunkType(9, 6)};
const ChunkType xor_7_of_9{ChunkType::getXorChunkType(9, 7)};
const ChunkType xor_8_of_9{ChunkType::getXorChunkType(9, 8)};
const ChunkType xor_9_of_9{ChunkType::getXorChunkType(9, 9)};
const ChunkType xor_p_of_9{ChunkType::getXorParityChunkType(9)};
