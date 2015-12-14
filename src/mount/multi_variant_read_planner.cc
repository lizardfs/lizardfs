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
#include "mount/multi_variant_read_planner.h"

#include <algorithm>
#include <iterator>
#include <limits>

#include "common/flat_map.h"
#include "common/slice_traits.h"

void MultiVariantReadPlanner::prepare(const std::vector<ChunkPartType>&) {
}

std::vector<ChunkPartType> MultiVariantReadPlanner::partsToUse() const {
	return std::vector<ChunkPartType>();
}

bool MultiVariantReadPlanner::isReadingPossible() const {
	return false;
}

std::unique_ptr<ReadPlan> MultiVariantReadPlanner::buildPlanFor(uint32_t, uint32_t) {
	return std::unique_ptr<ReadPlan>();
}

void MultiVariantReadPlanner::setScores(flat_map<ChunkPartType, float>) {
}

void MultiVariantReadPlanner::startAvoidingPart(ChunkPartType) {
}
