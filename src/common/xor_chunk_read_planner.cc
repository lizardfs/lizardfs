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
#include "common/xor_chunk_read_planner.h"

#include <algorithm>
#include <iostream>

#include "common/goal.h"
#include "common/slice_traits.h"
#include "common/standard_chunk_read_planner.h"

XorChunkReadPlanner::XorChunkReadPlanner(ChunkPartType) {
}

XorChunkReadPlanner::~XorChunkReadPlanner() {}

void XorChunkReadPlanner::prepare(const std::vector<ChunkPartType>&) {
}

std::vector<ChunkPartType> XorChunkReadPlanner::partsToUse() const {
	return std::vector<ChunkPartType>();
}

bool XorChunkReadPlanner::isReadingPossible() const {
	return false;
}

std::unique_ptr<ReadPlan> XorChunkReadPlanner::buildPlanFor(
		uint32_t, uint32_t) {
	return std::unique_ptr<ReadPlan>();
}
