#pragma once

#include "common/platform.h"


#include "common/massert.h"
#include "common/read_planner.h"

/**
 * ReadPlanner::Plan for a single variant.
 * A class which handles only the basic variant of a read plan, ie. the one which needs all basic
 * read operations to be finished and has a hard-coded list of post-process operations for this
 * variant.
 */
class SingleVariantReadPlan : public ReadPlanner::Plan {
public:
	SingleVariantReadPlan() {}

	SingleVariantReadPlan(SingleVariantReadPlan&&) = default;

	SingleVariantReadPlan(std::unique_ptr<Plan> plan) {
		requiredBufferSize = plan->requiredBufferSize;
		postProcessOperations_ = plan->getPostProcessOperationsForBasicPlan();
		basicReadOperations = std::move(plan->basicReadOperations);
	}

	SingleVariantReadPlan(std::vector<ReadPlanner::PostProcessOperation> operations)
			: postProcessOperations_(std::move(operations)) {
	}

	bool isReadingFinished(const std::set<ChunkType>&) const override {
		return false;
	}

	std::vector<ReadPlanner::PostProcessOperation> getPostProcessOperationsForBasicPlan(
			) const override {
		return postProcessOperations_;
	}

	std::vector<ReadPlanner::PostProcessOperation> getPostProcessOperationsForExtendedPlan(
			const std::set<ChunkType>& unfinished) const override {
		sassert(isReadingFinished(unfinished));
		return {};
	}

	/**
	 * Adds an operation to list returned by \p getPostProcessOperationsForBasicPlan.
	 * \param op    operation to be added
	 */
	void addPostProcessOperation(ReadPlanner::PostProcessOperation op) {
		postProcessOperations_.push_back(std::move(op));
	}

	/**
	 * A getter which allows to modify the list.
	 */
	std::vector<ReadPlanner::PostProcessOperation>& postProcessOpearations() {
		return postProcessOperations_;
	}

private:
	std::vector<ReadPlanner::PostProcessOperation> postProcessOperations_;
};
