#pragma once

#include "common/platform.h"

#include "common/read_planner.h"

class XorChunkReadPlanner : public ReadPlanner {
public:
	XorChunkReadPlanner(ChunkType readChunkType);
	~XorChunkReadPlanner();

	virtual void prepare(const std::vector<ChunkType>& availableParts,
			const std::map<ChunkType, float>& serverScores);
	virtual std::vector<ChunkType> partsToUse() const;
	virtual bool isReadingPossible() const;
	virtual std::unique_ptr<ReadPlanner::Plan> buildPlanFor(
			uint32_t firstBlock, uint32_t blockCount) const;

private:
	class PlanBuilder;
	class CopyPartPlanBuilder;
	class XorPlanBuilder;

	ChunkType chunkType_;
	std::vector<ChunkType> partsToUse_;
	std::unique_ptr<PlanBuilder> planBuilder_;
};
