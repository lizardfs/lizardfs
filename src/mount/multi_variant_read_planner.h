#pragma once

#include "common/platform.h"

#include "common/read_planner.h"
#include "common/standard_chunk_read_planner.h"

/**
 * A class which generates plans with multiple variants, eg. reading from all xor parts
 * and discarding the slowest one.
 */
class MultiVariantReadPlanner : public ReadPlanner {
public:
	// Derived methods
	void prepare(const std::vector<ChunkType>& availableParts) override;
	std::vector<ChunkType> partsToUse() const override;
	bool isReadingPossible() const override;
	std::unique_ptr<ReadPlan> buildPlanFor(
			uint32_t firstBlock, uint32_t blockCount) const override;

	/**
	 * Set scores of chunk types.
	 * The scores will be used to choose which variant should be the basic one.
	 */
	void setScores(std::map<ChunkType, float> scores);

	/// Modifies the planner to avoid using the given part in basic operations in the future.
	void startAvoidingPart(ChunkType part);

private:
	/// Parts used in plans (for both basic and additional read operations)
	std::set<ChunkType> partsToUse_;

	/// Scores which will be used in planning.
	std::map<ChunkType, float> scores_;

	/// Planner which will be used to generate plans.
	StandardChunkReadPlanner standardPlanner_;
};

