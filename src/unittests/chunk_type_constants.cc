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
#include "common/slice_traits.h"
#include "unittests/chunk_type_constants.h"

const ChunkPartType standard{slice_traits::standard::ChunkPartType()};
const ChunkPartType xor_1_of_2{slice_traits::xors::ChunkPartType(2, 1)};
const ChunkPartType xor_2_of_2{slice_traits::xors::ChunkPartType(2, 2)};
const ChunkPartType xor_p_of_2{slice_traits::xors::ChunkPartType(2, slice_traits::xors::kXorParityPart)};
const ChunkPartType xor_1_of_3{slice_traits::xors::ChunkPartType(3, 1)};
const ChunkPartType xor_2_of_3{slice_traits::xors::ChunkPartType(3, 2)};
const ChunkPartType xor_3_of_3{slice_traits::xors::ChunkPartType(3, 3)};
const ChunkPartType xor_p_of_3{slice_traits::xors::ChunkPartType(3, slice_traits::xors::kXorParityPart)};
const ChunkPartType xor_1_of_4{slice_traits::xors::ChunkPartType(4, 1)};
const ChunkPartType xor_2_of_4{slice_traits::xors::ChunkPartType(4, 2)};
const ChunkPartType xor_3_of_4{slice_traits::xors::ChunkPartType(4, 3)};
const ChunkPartType xor_4_of_4{slice_traits::xors::ChunkPartType(4, 4)};
const ChunkPartType xor_p_of_4{slice_traits::xors::ChunkPartType(4, slice_traits::xors::kXorParityPart)};
const ChunkPartType xor_1_of_6{slice_traits::xors::ChunkPartType(6, 1)};
const ChunkPartType xor_2_of_6{slice_traits::xors::ChunkPartType(6, 2)};
const ChunkPartType xor_3_of_6{slice_traits::xors::ChunkPartType(6, 3)};
const ChunkPartType xor_4_of_6{slice_traits::xors::ChunkPartType(6, 4)};
const ChunkPartType xor_5_of_6{slice_traits::xors::ChunkPartType(6, 5)};
const ChunkPartType xor_6_of_6{slice_traits::xors::ChunkPartType(6, 6)};
const ChunkPartType xor_p_of_6{slice_traits::xors::ChunkPartType(6, slice_traits::xors::kXorParityPart)};
const ChunkPartType xor_1_of_7{slice_traits::xors::ChunkPartType(7, 1)};
const ChunkPartType xor_2_of_7{slice_traits::xors::ChunkPartType(7, 2)};
const ChunkPartType xor_3_of_7{slice_traits::xors::ChunkPartType(7, 3)};
const ChunkPartType xor_4_of_7{slice_traits::xors::ChunkPartType(7, 4)};
const ChunkPartType xor_5_of_7{slice_traits::xors::ChunkPartType(7, 5)};
const ChunkPartType xor_6_of_7{slice_traits::xors::ChunkPartType(7, 6)};
const ChunkPartType xor_7_of_7{slice_traits::xors::ChunkPartType(7, 7)};
const ChunkPartType xor_p_of_7{slice_traits::xors::ChunkPartType(7, slice_traits::xors::kXorParityPart)};
const ChunkPartType xor_1_of_9{slice_traits::xors::ChunkPartType(9, 1)};
const ChunkPartType xor_2_of_9{slice_traits::xors::ChunkPartType(9, 2)};
const ChunkPartType xor_3_of_9{slice_traits::xors::ChunkPartType(9, 3)};
const ChunkPartType xor_4_of_9{slice_traits::xors::ChunkPartType(9, 4)};
const ChunkPartType xor_5_of_9{slice_traits::xors::ChunkPartType(9, 5)};
const ChunkPartType xor_6_of_9{slice_traits::xors::ChunkPartType(9, 6)};
const ChunkPartType xor_7_of_9{slice_traits::xors::ChunkPartType(9, 7)};
const ChunkPartType xor_8_of_9{slice_traits::xors::ChunkPartType(9, 8)};
const ChunkPartType xor_9_of_9{slice_traits::xors::ChunkPartType(9, 9)};
const ChunkPartType xor_p_of_9{slice_traits::xors::ChunkPartType(9, slice_traits::xors::kXorParityPart)};
