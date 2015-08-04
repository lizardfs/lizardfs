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


#include "common/massert.h"
#include "common/read_planner.h"

/**
 * ReadPlan for a single variant.
 * A class which handles only the basic variant of a read plan, ie. the one which needs all basic
 * read operations to be finished and has a hard-coded list of post-process operations for this
 * variant.
 */
class SingleVariantReadPlan : public ReadPlan {
public:
	SingleVariantReadPlan() {}

	SingleVariantReadPlan(SingleVariantReadPlan&&) = default;

	SingleVariantReadPlan(std::unique_ptr<ReadPlan> plan) {
		requiredBufferSize = plan->requiredBufferSize;
		postProcessOperations_ = plan->getPostProcessOperationsForBasicPlan();
		basicReadOperations = std::move(plan->basicReadOperations);
	}

	SingleVariantReadPlan(std::vector<ReadPlan::PostProcessOperation> operations)
			: postProcessOperations_(std::move(operations)) {
	}

	bool isReadingFinished(const std::set<ChunkPartType>&) const override {
		return false;
	}

	std::vector<ReadPlan::PostProcessOperation> getPostProcessOperationsForBasicPlan(
			) const override {
		return postProcessOperations_;
	}

	std::vector<ReadPlan::PostProcessOperation> getPostProcessOperationsForExtendedPlan(
			const std::set<ChunkPartType>& unfinished) const override {
		sassert(isReadingFinished(unfinished));
		return {};
	}

	/**
	 * Adds an operation to list returned by \p getPostProcessOperationsForBasicPlan.
	 * \param op    operation to be added
	 */
	void addPostProcessOperation(ReadPlan::PostProcessOperation op) {
		postProcessOperations_.push_back(std::move(op));
	}

	/**
	 * A getter which allows to modify the list.
	 */
	std::vector<ReadPlan::PostProcessOperation>& postProcessOpearations() {
		return postProcessOperations_;
	}

private:
	std::vector<ReadPlan::PostProcessOperation> postProcessOperations_;
};
