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

#include "common/platform.h"

#include "common/slice_read_planner.h"

#include <cmath>

/*!
 * Prepares read planner for serving selected parts of a slice type.
 * Firstly, function checks if:
 *  - all requested parts are available,
 *  - or requested parts can be recovered,
 *  - or reading is not possible at all.
 * Then, parts are sorted by their score.
 * \param slice_type slice type for read operation
 * \param slice_parts requested parts of selected slice type
 * \param available_parts parts available in the system
 */
void SliceReadPlanner::prepare(Goal::Slice::Type slice_type, const PartIndexContainer &slice_parts,
		const PartsContainer &available_parts) {
	reset(slice_type, slice_parts);
	std::bitset<Goal::Slice::kMaxPartsCount> part_bitset;
	for (const auto &part : available_parts) {
		if (part.getSliceType() == slice_type_) {
			part_bitset.set(part.getSlicePart());
		}
	}

	required_parts_available_ =
	    std::all_of(slice_parts_.begin(), slice_parts_.end(),
	        [&part_bitset](int slice_part) {return part_bitset.test(slice_part);});

	can_recover_parts_ = (int)part_bitset.count() >= slice_traits::requiredPartsToRecover(slice_type_);
	can_read_ = required_parts_available_ || can_recover_parts_;

	if (!can_read_) {
		return;
	}

	for (const auto &part : available_parts) {
		if (part.getSliceType() == slice_type_ && part_bitset[part.getSlicePart()]) {
			auto it = scores_.find(part);
			float score = it != scores_.end() ? it->second : 1;
			weighted_parts_to_use_.push_back({score, part});
			part_bitset.reset(part.getSlicePart());
		}
	}

	std::stable_sort(
		weighted_parts_to_use_.begin(), weighted_parts_to_use_.end(),
		[](const WeightedPart &a, const WeightedPart &b) { return a.score > b.score; });
}

/*!
 * Adds read operations to plan.
 * If part was one of the requested ones, its buffer offset is determined by its order.
 * Additional parts have consecutive offsets starting after last requested part.
 * \param plan - plan to be amended
 * \param first_block first block to be read
 * \param block_count number of blocks to be read
 * \param parts_count number of parts to be added
 * \param wave wave number for read operation
 * \param buffer_offset current offset for additional parts
 * \return offset for additional parts
 */
int SliceReadPlanner::addParts(SliceReadPlan *plan, int first_block, int block_count,
		int parts_count, int wave, int buffer_offset) {
	int ops = plan->read_operations.size();
	int end = std::min<int>(ops + parts_count, weighted_parts_to_use_.size());

	for (; ops < end; ++ops) {
		ReadPlan::ReadOperation op{first_block * MFSBLOCKSIZE, 0, 0, wave};
		op.request_size = MFSBLOCKSIZE * std::min(
				(int)slice_traits::getNumberOfBlocks(weighted_parts_to_use_[ops].type) - first_block,
				block_count);
		int index = part_indices_[weighted_parts_to_use_[ops].type.getSlicePart()];
		if (index < 0) {
			op.buffer_offset = buffer_offset;
			buffer_offset += block_count * MFSBLOCKSIZE;
		} else {
			op.buffer_offset = index * MFSBLOCKSIZE * block_count;
		}
		plan->read_operations.push_back({weighted_parts_to_use_[ops].type, op});
	}

	plan->read_buffer_size = buffer_offset;
	return buffer_offset;
}

/*!
 * Adds read operations for parts to plan with highest priority (wave 0).
 * \param plan - plan to be amended
 * \param first_block first block to be read
 * \param block_count number of blocks to be read
 * \param parts_count number of parts to be added
 * \return buffer offset for additional parts
 */
int SliceReadPlanner::addBasicParts(SliceReadPlan *plan, int first_block, int block_count,
		int parts_count) {
	int buffer_offset = plan->requested_parts.size() * plan->buffer_part_size;
	return addParts(plan, first_block, block_count, parts_count, 0, buffer_offset);
}

/*!
 * Adds additional read operations to plan.
 * \param plan - plan to be amended
 * \param first_block first block to be read
 * \param block_count number of blocks to be read
 * \return buffer offset for additional parts
 */
int SliceReadPlanner::addExtraParts(SliceReadPlan *plan, int first_block, int block_count,
		int buffer_offset) {
	int ops = plan->read_operations.size();
	size_t all_parts_count = weighted_parts_to_use_.size();
	int to_recover = std::min<int>(
			std::floor(bandwidth_overuse_ * slice_traits::requiredPartsToRecover(slice_type_)),
			all_parts_count);
	int wave = 1;

	// Add parts needed to recover to wave 1
	if (ops < to_recover) {
		buffer_offset = addParts(plan, first_block, block_count, to_recover - ops, wave, buffer_offset);
		wave += 1;
	}

	// Add the rest with rising waves
	while (plan->read_operations.size() < all_parts_count) {
		int parts_count = std::min<int>(2, all_parts_count - plan->read_operations.size());
		buffer_offset = addParts(plan, first_block, block_count, parts_count, wave, buffer_offset);
		wave += 1;
	}

	return buffer_offset;
}

bool SliceReadPlanner::shouldReadPartsRequiredForRecovery() const {
	return !required_parts_available_;
}

/*!
 * Builds a read plan.
 * First step is preparing an occurrence bitmap of requested parts.
 * Then, if parts needed for recovery are to be read (recovery is needed or nearly all parts
 * were requested anyway), they are queued for reading. Otherwise, requested parts
 * are queued for first wave and additional parts are added to consecutive waves.
 * \param first_block first block to be read
 * \param block_count number of blocks to be read
 * \return read plan ready to be executed
 */
std::unique_ptr<ReadPlan> SliceReadPlanner::buildPlanFor(uint32_t first_block,
		uint32_t block_count) {
	std::unique_ptr<SliceReadPlan> plan = getPlan();
	plan->buffer_part_size = block_count * MFSBLOCKSIZE;

	// Count occurrences
	std::bitset<Goal::Slice::kMaxPartsCount> part_bitset;
	for (const auto &part : slice_parts_) {
		assert(!part_bitset[part]);
		part_bitset.set(part);
	}

	// Prepare indices for each requested part
	int next_index = 0;
	part_indices_.fill(-1);
	for (const auto &part : slice_parts_) {
		int size = MFSBLOCKSIZE * std::min<int>(
			slice_traits::getNumberOfBlocks(ChunkPartType(slice_type_, part)) - first_block,
			block_count);
		plan->requested_parts.push_back({part, size});
		part_indices_[part] = next_index++;
	}

	if (shouldReadPartsRequiredForRecovery()) {
		int recovery_parts_count = std::max<int>(
			std::floor(bandwidth_overuse_ *
				(required_parts_available_ ?
					slice_parts_.size() : slice_traits::requiredPartsToRecover(slice_type_))),
			slice_traits::requiredPartsToRecover(slice_type_));

		int offset = addBasicParts(plan.get(), first_block, block_count, recovery_parts_count);
		addExtraParts(plan.get(), first_block, block_count, offset);
	} else {
		auto breakpoint =
		    std::stable_partition(weighted_parts_to_use_.begin(),
		        weighted_parts_to_use_.end(), [&part_bitset](const WeightedPart &part) {
			        return part_bitset[part.type.getSlicePart()];
		        });

		int requested_parts_count = std::distance(weighted_parts_to_use_.begin(), breakpoint);
		int offset = addBasicParts(plan.get(), first_block, block_count, requested_parts_count);
		addExtraParts(plan.get(), first_block, block_count, offset);
	}

	return plan;
}
