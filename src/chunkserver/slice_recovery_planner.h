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

#include <cassert>
#include "common/chunk_read_planner.h"
#include "common/slice_read_planner.h"

/*! \brief Class creating read plan recovering single slice part.
 *
 * SliceRecoveryPlanner uses 3 methods for recovering chunk part.
 * - First method is to simply read data/parity part if it's possible (it may required recalculating
 * data from parity parts, but it is done by SliceReadPlanner class).
 * - Second type is to read chunk data using ChunkReadPlanner (maybe from some other type of slice)
 * and then convert data by use of BlockConverter post-processing functor.
 * - Third method is for parity data where ChunkReadPlanner is used to read chunk data and
 * later XorReadPlan::RecoverParity (or ECReadPlan::RecoverParity) functor is used
 * to calculate parity information.
 */
class SliceRecoveryPlanner {
protected:
	/*! \brief Copies blocks that belong to chunk part from chunk data. */
	struct BlockConverter {
		BlockConverter() {}
		BlockConverter(const BlockConverter &) = default;
		BlockConverter(BlockConverter &&) = default;

		void operator()(uint8_t *dst, int, const uint8_t *src, int) const {
			assert(plan);
			for (int i = 0; i < part_block_count; ++i) {
				assert(dst >= plan->buffer_start && (dst + MFSBLOCKSIZE) <= plan->buffer_read);
				assert(src >= plan->buffer_start && (src + MFSBLOCKSIZE) <= plan->buffer_end);
				std::memcpy(dst, src, MFSBLOCKSIZE);
				dst += MFSBLOCKSIZE;
				src += data_part_count * MFSBLOCKSIZE;
			}
		}

		int part_block_count; /*!< Number of blocks in chunk part that should be recovered. */
		int data_part_count; /*!< Number of data parts for slice type of recovered part. */

#ifndef NDEBUG
		ReadPlan *plan;
#endif
	};

	enum {
		kReadDataPart,     /*!< Recovery method: Read data/parity from the same slice type. */
		kRecoverDataPart,  /*!< Recovery method: Use chunk data to recover part. */
		kRecoverParityPart /*!< Recovery method: Use chunk data and parity calculator to recover
	                          part. */
	};

public:
	typedef ChunkReadPlanner::PartsContainer PartsContainer;
	typedef ChunkReadPlanner::ScoreContainer ScoreContainer;

	SliceRecoveryPlanner() {}

	/*! \brief Prepare for creating plan.
	 *
	 * \param chunk_part Chunk part type to recover.
	 * \param first_block First Block in part to recover.
	 * \param block_count Number of blocks to recover.
	 * \param available_parts Container with types of available chunk parts.
	 */
	void prepare(ChunkPartType chunk_part, int first_block, int block_count,
	             const PartsContainer &available_parts) {
		SliceReadPlanner::PartIndexContainer parts;

		recovering_part_ = chunk_part;
		first_block_ = first_block;
		block_count_ = block_count;
		is_reading_possible_ = false;

		parts.push_back(chunk_part.getSlicePart());
		slice_planner_.prepare(chunk_part.getSliceType(), parts, available_parts);
		if (slice_planner_.isReadingPossible()) {
			is_reading_possible_ = true;
			recovery_type_ = kReadDataPart;
		} else if (slice_traits::isDataPart(chunk_part)) {
			int data_part = slice_traits::getDataPartIndex(chunk_part);
			int data_part_count = slice_traits::getNumberOfDataParts(chunk_part);
			chunk_planner_.prepare(data_part + first_block * data_part_count,
			                       1 + (block_count - 1) * data_part_count, available_parts);
			if (chunk_planner_.isReadingPossible()) {
				is_reading_possible_ = true;
				recovery_type_ = kRecoverDataPart;
			}
		} else {
			int data_part_count = slice_traits::getNumberOfDataParts(chunk_part);
			chunk_planner_.prepare(first_block * data_part_count, block_count * data_part_count,
			                       available_parts);
			if (chunk_planner_.isReadingPossible()) {
				is_reading_possible_ = true;
				recovery_type_ = kRecoverParityPart;
			}
		}
	}

	/*! \brief Set chunk part scores.
	 *
	 * \param scores Container with chunk type scores. */
	void setScores(const ScoreContainer &scores) {
		slice_planner_.setScores(scores);
		chunk_planner_.setScores(scores);
	}

	/*! \brief Set chunk part scores.
	 *
	 * \param scores Container with chunk type scores. */
	void setScores(ScoreContainer &&scores) {
		slice_planner_.setScores(scores);
		chunk_planner_.setScores(std::move(scores));
	}

	/*! \brief Returns true if it's possible to read data from available chunk parts. */
	bool isReadingPossible() const {
		return is_reading_possible_;
	}

	/*! \brief Returns ReadPlan for recovering chunk part data.
	 *
	 * \return Unique pointer to created ReadPlan.
	 */
	std::unique_ptr<ReadPlan> buildPlan() {
		switch (recovery_type_) {
		case kReadDataPart:
			return slice_planner_.buildPlanFor(first_block_, block_count_);
		case kRecoverDataPart: {
			std::unique_ptr<ReadPlan> plan = chunk_planner_.buildPlan();

			BlockConverter conv;
			conv.part_block_count = block_count_;
			conv.data_part_count =
			    slice_traits::getNumberOfDataParts(recovering_part_.getSliceType());
#ifndef NDEBUG
			conv.plan = plan.get();
#endif

			plan->postprocess_operations.push_back(
			    std::make_pair(block_count_ * MFSBLOCKSIZE, std::move(conv)));

			return plan;
		}
		case kRecoverParityPart: {
			std::unique_ptr<ReadPlan> plan = chunk_planner_.buildPlan();

			assert(slice_traits::isXor(recovering_part_) &&
			       slice_traits::xors::isXorParity(recovering_part_));
			XorReadPlan::RecoverParity conv;

			conv.part_block_count = block_count_;
			conv.data_part_count =
			    slice_traits::requiredPartsToRecover(recovering_part_.getSliceType());
#ifndef NDEBUG
			conv.plan = plan.get();
#endif

			plan->postprocess_operations.push_back(
			    std::make_pair(block_count_ * MFSBLOCKSIZE, std::move(conv)));

			return plan;
		}
		default:
			assert(!"SliceRecoveryPlanner::buildPlan::It shouldn't happen.");
		};

		return nullptr;
	}

protected:
	SliceReadPlanner slice_planner_; /*!< Helper class for creating kReadDataPart recovery method. */
	ChunkReadPlanner chunk_planner_; /*!< Helper class for creating kRecoverDataPart and kRecoverParityPart recovery methods. */
	ChunkPartType recovering_part_; /*!< Chunk part type to be recovered. */
	bool is_reading_possible_; /*!< Chunk part type to be recovered. */
	int recovery_type_; /*!< Required recovery type (read data part, recover part, recover parity). */
	int first_block_; /*!< First chunk block to recover. */
	int block_count_; /*!< Number of chunk blocks to recover. */
};
