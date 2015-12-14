/*
 Copyright 2015 Skytechnology sp. z o.o.

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

#include <bitset>
#include <cassert>
#include <utility>

#include "common/flat_map.h"
#include "common/goal.h"
#include "common/slice_read_plan.h"
#include "common/small_vector.h"

/*!
 * Class responsible for creating a plan for reading selected parts from a slice.
 *
 * Planner is initialized with a slice type, list of requested parts and list of available parts.
 * Plan is created depending on parts that are available in the system.
 *
 * If all requested parts are available, they will be requested in the first wave.
 * Additional parts for anticipated recovery are scheduled for next waves.
 * If enough recovery parts arrive before all requested ones, then, instead of waiting
 * for missing requested parts, they are obtained by recovery mechanism.
 *
 * If some/all of requested parts are not available, an amount of parts needed for recovery
 * is scheduled for the first wave. Additional parts are queued for consecutive waves.
 *
 * If number of requested parts is near to the number of parts needed for recovery,
 * they are ale scheduled for the first wave.
 */
class SliceReadPlanner {
public:
	typedef ReadPlan::PartsContainer PartsContainer;
	typedef small_vector<int, Goal::Slice::kMaxPartsCount / 2> PartIndexContainer;
	typedef flat_map<ChunkPartType, float,
	                 small_vector<std::pair<ChunkPartType, float>, Goal::Slice::kMaxPartsCount / 2>>
	    ScoreContainer;

	SliceReadPlanner() : slice_type_(0), slice_parts_(), weighted_parts_to_use_(), scores_(),
			bandwidth_overuse_(1.25),
			can_read_(), required_parts_available_(), can_recover_parts_(), part_indices_() {
	}

	/*!
	 * Initializes a planner with information on requested and available parts.
	 * \param slice_type slice type selected for read operation
	 * \param slice_parts parts requested to be read
	 * \param available_parts parts available in the system
	 */
	void prepare(Goal::Slice::Type slice_type, const PartIndexContainer &slice_parts,
	             const PartsContainer &available_parts);

	void setScores(const ScoreContainer &scores) {
		scores_ = scores;
	}

	void setScores(ScoreContainer &&scores) {
		scores_ = std::move(scores);
	}

	bool isReadingPossible() const {
		return can_read_;
	}

	/*!
	 * Builds a plan for reading blocks in range [first_block, first_block + block_count).
	 */
	std::unique_ptr<ReadPlan> buildPlanFor(uint32_t first_block, uint32_t block_count);

private:
	struct WeightedPart {
		float score;
		ChunkPartType type;
	};

	typedef ReadPlan::ReadOperation ReadOperation;
	typedef small_vector<WeightedPart, Goal::Slice::kMaxPartsCount/2> WeightedPartsContainer;

	void assignScores() {
		for (auto &part : weighted_parts_to_use_) {
			auto it = scores_.find(part.type);
			if (it != scores_.end()) {
				part.score = it->second;
			}
		}
	}

	bool shouldReadPartsRequiredForRecovery() const;

	int addParts(SliceReadPlan *plan, int first_block, int block_count, int parts_count,
		int wave, int buffer_offset);
	int addBasicParts(SliceReadPlan *plan, int first_block, int block_count, int parts_count);
	int addExtraParts(SliceReadPlan *plan, int first_block, int block_count, int buffer_offset);

	void reset(Goal::Slice::Type slice_type, const PartIndexContainer &slice_parts) {
		slice_type_ = slice_type;
		slice_parts_ = slice_parts;
		weighted_parts_to_use_.clear();
		can_read_ = false;
		required_parts_available_ = false;
		can_recover_parts_ = false;
	}

	std::unique_ptr<SliceReadPlan> getPlan() const {
		return std::unique_ptr<SliceReadPlan>{new SliceReadPlan(slice_type_)};
	}

	Goal::Slice::Type slice_type_;
	PartIndexContainer slice_parts_;
	WeightedPartsContainer weighted_parts_to_use_;
	ScoreContainer scores_;
	float bandwidth_overuse_;
	bool can_read_;
	bool required_parts_available_;
	int can_recover_parts_;
	std::array<int, Goal::Slice::kMaxPartsCount> part_indices_;
};
