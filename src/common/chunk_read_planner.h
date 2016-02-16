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
#include <boost/concept_check.hpp>
#include "common/slice_read_planner.h"

#include <iostream>

/*! \brief This class creates plan for reading chunk data in continuous memory region.
 *
 * Chunk data can be split into multiple chunk parts (with different slice types).
 * This requires selecting proper parts and merging them to achieve continuous data block.
 */
class ChunkReadPlanner {
protected:
	/*! Functor for converting split chunk data into continuous region. */
	struct BlockConverter {
		BlockConverter() {}
		BlockConverter(const BlockConverter &) = default;
		BlockConverter(BlockConverter &&) = default;

		void operator()(uint8_t *dst, int, const uint8_t *src, int) const {
			assert(plan);
			for (int i = 0; i < chunk_block_count; ++i) {
				int block = (chunk_first_block + i) / data_part_count - part_first_block;
				int part = (chunk_first_block + i) % data_part_count - first_required_part;
				if (part < 0) {
					part += data_part_count;
				}

				assert(block >= 0);

				assert(dst >= plan->buffer_start && (dst + MFSBLOCKSIZE) <= plan->buffer_read);
				assert(src >= plan->buffer_read && (src + MFSBLOCKSIZE) <= plan->buffer_end);
				std::memcpy(dst, src + (part * part_block_count + block) * MFSBLOCKSIZE,
				            MFSBLOCKSIZE);
				dst += MFSBLOCKSIZE;
			}
		}

		int chunk_first_block; /*!< Index of first block in chunk to read. */
		int chunk_block_count; /*!< Number of blocks in chunk to read. */
		int part_first_block; /*!< Index of first block in chunk part to read. */
		int part_block_count; /*!< Number of blocks in chunk part to read. */
		int first_required_part; /*!< Index of first chunk part to read. */
		int data_part_count; /*!< Number of data parts for slice type that chunk parts belong to. */

#ifndef NDEBUG
		ReadPlan *plan;
#endif
	};

public:
	typedef SliceReadPlanner::ScoreContainer ScoreContainer;
	typedef SliceReadPlanner::PartsContainer PartsContainer;

	ChunkReadPlanner(double bandwidth_overuse = 1.)
		: read_planner_(bandwidth_overuse), read_from_type_(0) {
	}

	/*! \brief Prepare for creating plan.
	 *
	 * \param first_block First block of chunk that should be read.
	 * \param block_count Number of blocks to read.
	 * \param available_parts Container with types of available chunk parts.
	 */
	void prepare(int first_block, int block_count, const PartsContainer &available_parts) {
		small_vector<Goal::Slice::Type, 10> scan_type;

		assert(first_block >= 0 && block_count > 0 && (first_block + block_count));

		getTypeList(scan_type, available_parts);

		// TODO(haze): If not necessary don't read more blocks in multi-lane read request.
		//             This requires modifications to SliceReadPlanner.
		// TODO(haze): Create plan score and use it to select best plan.
		for (const auto &type : scan_type) {
			getRequiredParts(read_parts_, type, first_block, block_count);
			read_planner_.prepare(type, read_parts_, available_parts);
			if (read_planner_.isReadingPossible()) {
				int data_parts = slice_traits::requiredPartsToRecover(type);
				read_from_type_ = type;
				part_first_block_ = first_block / data_parts;
				part_block_count_ = (first_block + block_count - 1) / data_parts - part_first_block_ + 1;
				chunk_first_block_ = first_block;
				chunk_block_count_ = block_count;
				is_reading_possible_ = true;
				return;
			}
		}

		is_reading_possible_ = false;
	}

	/*! \brief Set chunk part scores.
	 *
	 * \param scores Container with chunk type scores. */
	void setScores(const ScoreContainer &scores) {
		read_planner_.setScores(scores);
	}

	/*! \brief Set chunk part scores.
	 *
	 * \param scores Container with chunk type scores. */
	void setScores(ScoreContainer &&scores) {
		read_planner_.setScores(std::move(scores));
	}

	/*! \brief Returns true if it's possible to read data from available chunk parts. */
	bool isReadingPossible() const {
		return is_reading_possible_;
	}

	/*! \brief Returns ReadPlan for reading chunk data from available chunk parts.
	 *
	 * \return Unique pointer to created ReadPlan.
	 */
	std::unique_ptr<ReadPlan> buildPlan() {
		std::unique_ptr<ReadPlan> plan;

		plan = read_planner_.buildPlanFor(part_first_block_, part_block_count_);
		if (!plan || slice_traits::isStandard(read_from_type_)) {
			return plan;
		}

		BlockConverter conv;

		conv.chunk_first_block = chunk_first_block_;
		conv.chunk_block_count = chunk_block_count_;
		conv.part_first_block = part_first_block_;
		conv.part_block_count = part_block_count_;
		conv.first_required_part =
		    slice_traits::isXor(read_from_type_) ? read_parts_[0] - 1 : read_parts_[0];
		conv.data_part_count = slice_traits::requiredPartsToRecover(read_from_type_);
#ifndef NDEBUG
		conv.plan = plan.get();
#endif

		plan->postprocess_operations.push_back(
		    std::make_pair(chunk_block_count_ * MFSBLOCKSIZE, std::move(conv)));

		return std::move(plan);
	}

protected:
	template <class R, class V>
	void getTypeList(R &result, const V &available_parts) const {
		result.clear();
		for (const auto &part : available_parts) {
			if (std::find(result.begin(), result.end(), part.getSliceType()) == result.end()) {
				result.push_back(part.getSliceType());
			}
		}
	}

	/*! \brief Get vector with required parts to read chunk data for [first_block, first_block + block_count) range.
	 *
	 * \param result Output vector with required parts.
	 * \param type Slice type of parts that should be read.
	 * \param first_block First block of range to read.
	 * \param block_count Number of blocks to read.
	 */
	void getRequiredParts(SliceReadPlanner::PartIndexContainer &result, Goal::Slice::Type type, int first_block,
	                      int block_count) const {
		int data_part_count = slice_traits::requiredPartsToRecover(type);
		int first_data_part = slice_traits::isXor(type) ? 1 : 0;

		int first_part = first_block % data_part_count;
		int last_part = (first_block + block_count - 1) % data_part_count;

		if (block_count >= data_part_count) {
			result.resize(data_part_count);
			std::iota(result.begin(), result.end(), first_data_part);
			return;
		}

		if (first_part <= last_part) {
			result.resize(last_part - first_part + 1);
			std::iota(result.begin(), result.end(), first_part + first_data_part);
			return;
		}

		result.resize(data_part_count - first_part + last_part + 1);
		std::iota(result.begin(), result.begin() + (data_part_count - first_part),
		          first_part + first_data_part);
		std::iota(result.begin() + (data_part_count - first_part), result.end(), first_data_part);
	}

	SliceReadPlanner read_planner_; /*!< Helper class used for creating base read plan. */
	bool is_reading_possible_; /*!< True if it is possible to read data from specified chunk parts. */
	Goal::Slice::Type read_from_type_; /*!< Slice type of chunk parts to read. */
	SliceReadPlanner::PartIndexContainer read_parts_; /*!< Vector with indices of parts to read. */
	int part_first_block_; /*!< First block of chunk parts to read. */
	int part_block_count_; /*!< Number of blocks for each chunk part to read. */
	int chunk_first_block_; /*!< First block of chunk to read. */
	int chunk_block_count_; /*!< Number of blocks in chunk to read. */
};
