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

#pragma once

#include "common/platform.h"

#include "common/read_planner.h"

class XorChunkReadPlanner : public ReadPlanner {
public:
	XorChunkReadPlanner(ChunkPartType readChunkType);
	~XorChunkReadPlanner();

	virtual void prepare(const std::vector<ChunkPartType>& availableParts) override;
	virtual std::vector<ChunkPartType> partsToUse() const override;
	virtual bool isReadingPossible() const override;
	virtual std::unique_ptr<ReadPlan> buildPlanFor(
			uint32_t firstBlock, uint32_t blockCount) const override;

private:
	class PlanBuilder;
	class CopyPartPlanBuilder;
	class XorPlanBuilder;

	ChunkPartType chunkType_;
	std::vector<ChunkPartType> partsToUse_;
	std::unique_ptr<PlanBuilder> planBuilder_;
};
