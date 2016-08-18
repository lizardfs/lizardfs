/*
   Copyright 2015-2017 Skytechnology sp. z o.o.

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

#include <vector>

#include "common/chunk_part_type.h"
#include "common/chunks_availability_state.h"
#include "common/goal.h"
#include "common/flat_map.h"
#include "common/media_label.h"

/*! \brief Class used to calculate required operations to achieve chunk goal.
 *
 * This class uses two variables "available_" and "target_" to
 * represent current and desired state of the chunk parts.
 *
 * Lifetime of ChunkCopiesCalculator object can be divided into 3 stages.
 * 1. Set state of available parts and target goal.
 * 2. Make target optimization to reduce number of required chunk operations.
 * 3. Query calculator to get information on required chunk operations to achieve target goal.
 */
class ChunkCopiesCalculator {
	typedef flat_map<
	    Goal::Slice::Type, small_vector<std::pair<int, int>, Goal::Slice::kMaxPartsCount>,
	    small_vector<std::pair<Goal::Slice::Type,
	                           small_vector<std::pair<int, int>, Goal::Slice::kMaxPartsCount>>,
	                 5>> SliceOpCountContainer;
	typedef flat_map<Goal::Slice::Type, int,
	                 small_vector<std::pair<Goal::Slice::Type, int>, 5>>
	    SliceStateContainer;

public:
	/*! Default constructor. */
	ChunkCopiesCalculator();

	/*! \brief Constructor setting target goal.
	 * \param target goal describing desired chunk state.
	 */
	ChunkCopiesCalculator(Goal target);

	/*! \brief Set target goal.
	 * \param target goal describing desired chunk state.
	 */
	void setTarget(Goal target);

	/*! \brief Remove chunk part from available parts.
	 * \param slice_type slice type of chunk part to be removed.
	 * \param part slice part number of chunk part to be removed.
	 * \param label label of chunk server occupied by chunk part.
	 */
	void removePart(const Goal::Slice::Type &slice_type, int part, const MediaLabel &label);

	/*! \brief Optimize target goal to reduce number of required chunk operations.
	 *
	 * This function computes parts permutation (for each slide) to reduce
	 * number of required chunk operations to achieve target goal.
	 * As by-product this functions also computes chunk state (executes evalRedundancyLevel)
	 * and stores number or required chunk ops for later use.
	 *
	 * All query functions might be used only after this function has been executed.
	 */
	void optimize();

	/*! \brief Evaluate chunk redundancy level.
	 *
	 * This function evaluates number of redundant (above required) chunk parts that are
	 * needed for a Chunk to be considered safe.
	 *
	 * X number of redundant chunk parts indicates that X parts can disappear and chunk is not lost.
	 * 0 redundant chunk parts means that chunk is endangered and losing one chunk part
	 * leads to losing some of chunk data.
	 * Negative number of redundant chunk parts means that part of chunk data is not available.
	 *
	 * State query functions may be used only after this function has been executed.
	 */
	void evalRedundancyLevel();

	/*! \brief Check whether state of the chunk meets given safety requirement
	 *
	 * This function evaluates safety of the chunk comparing it to the
	 * requirement given as parameter. In case of target goal which by definition
	 * has less redundant chunk parts than given minimum, chunk is always considered
	 * safe enough.
	 *
	 * \param min_redundant_parts minimum number of redundant chunk parts for chunk to be
	 *                            considered safe.
	 * \return true if state of chunk is safe enough, otherwise false.
	 */
	bool isSafeEnoughToWrite(int min_redundant_parts);

	/*! \brief Update chunk redundancy level after modifications to one slice.
	 *
	 * This function may be used only after evalRedundancyLevel has been run.
	 *
	 * \param slice_type type of slice that has been modified.
	 */
	void updateRedundancyLevel(const Goal::Slice::Type &slice_type);

	/*! \brief Query if extra chunk part may be safely removed.
	 *
	 * This functions checks if removing specified chunk part
	 * doesn't make the chunk endangered.
	 *
	 * \param slice_type type of slice to check.
	 * \param part number of slice part.
	 * \param label label of chunk server occupied by chunk part.
	 * \return true if chunk will be in safe state after removing selected chunk part.
	 */
	bool canRemovePart(const Goal::Slice::Type &slice_type, int part,
	                   const MediaLabel &label) const;

	/*! \brief Check if chunk part may be moved to other label without violating target goal.
	 *
	 * \param slice_type type of slice to check.
	 * \param part number of slice part.
	 * \param label label of chunk server occupied by chunk part.
	 * \return true if chunk part may be safely moved.
	 */
	bool canMovePartToDifferentLabel(const Goal::Slice::Type &slice_type, int part,
	                                 const MediaLabel &label) const;

	/*! \brief Get labels that need to be recovered.
	 *
	 * \param slice_type type of slice to check.
	 * \param part number of slice part.
	 * \return Labels map describing chunk parts that need to be recovered.
	 */
	Goal::Slice::Labels getLabelsToRecover(const Goal::Slice::Type &slice_type, int part) const;

	/*! \brief Get pool of labels from witch only one label can be removed.
	 *
	 * This function returns pool of labels. Only one chunk part on server matching
	 * label from pool can be safely removed. This function is required because sometimes
	 * there is a choice between between two labels. One of them can be removed
	 * but not both. Using pool allows to choose from those labels
	 * using arbitrary criteria (like disk usage).
	 *
	 * \param slice_type type of slice to check.
	 * \param part number of slice part.
	 * \return Labels map describing chunk parts pool.
	 */
	Goal::Slice::Labels getRemovePool(const Goal::Slice::Type &slice_type, int part) const;

	/*! \brief Return number of chunk parts that need to be recovered and removed.
	 *
	 * \param slice_type type of slice to check.
	 * \param part number of slice part.
	 * \return {number of chunk parts to recover, number of chunk parts to remove}
	 */
	std::pair<int, int> countPartsToMove(const Goal::Slice::Type &slice_type, int part) const;

	/*! \brief Return number of full copies of chunk data.
	 *
	 * Rules for counting full copies of chunk data depend on slice type.
	 * For standard type each chunk contains full copy of chunk data so each chunk part is
	 * included in final count.
	 * For xorN we need all N chunk parts for full copy of chunk data and we count them as one.
	 *
	 * \return Number of full copies of chunk data.
	 */
	int getFullCopiesCount() const {
		return getFullCopiesCount(available_);
	}

	/*! \brief Return number of full copies of chunk data in goal.
	 *
	 * \param goal goal to check.
	 * \return Number of full copies of chunk data.
	 */
	static int getFullCopiesCount(const Goal& goal);

	/*! \brief Add specified chunk part to available parts.
	 *
	 * \param slice_type type of slice to check.
	 * \param part number of slice part.
	 * \param label label of chunk server occupied by chunk part.
	 */
	void addPart(const Goal::Slice::Type &slice_type, int part, const MediaLabel &label) {
		available_[slice_type][part][label]++;
	}

	/*! \brief Add specified chunk part to available parts.
	 *
	 * \param part_type chunk part type.
	 * \param label label of chunk server occupied by chunk part.
	 */
	void addPart(const ChunkPartType &part_type, const MediaLabel &label) {
		addPart(part_type.getSliceType(), part_type.getSlicePart(), label);
	}

	/*! \brief Return redundancy level of chunk. */
	int getRedundancyLevel() const {
		return redundancy_level_;
	}

	/*! \brief Return chunk state. */
	ChunksAvailabilityState::State getState() const {
		if (target_.size() == 0 || redundancy_level_ > 0) {
			return ChunksAvailabilityState::kSafe;
		}
		if (redundancy_level_ == 0) {
			return ChunksAvailabilityState::kEndangered;
		}
		return ChunksAvailabilityState::kLost;
	}

	/*! \brief Get goal with available parts. */
	Goal &getAvailable() {
		return available_;
	}

	/*! \brief Get goal with desired chunk parts. */
	Goal &getTarget() {
		return target_;
	}

	/*! \brief Returns number of chunk parts that need to be recovered. */
	int countPartsToRecover() const {
		return operation_count_.first;
	}

	/*! \brief Returns number of chunk parts that need to be removed. */
	int countPartsToRemove() const {
		return operation_count_.second;
	}

	/*! \brief Returns true if chunk can be recovered. */
	bool isRecoveryPossible() const {
		return getState() != ChunksAvailabilityState::kLost;
	}

	/*! \brief Returns true if chunk can be written. */
	bool isWritingPossible() const {
		// don't allow writing to unrecoverable chunks
		return getState() != ChunksAvailabilityState::kLost;
	}

protected:
	std::pair<int, int> operationCount(const Goal::Slice::ConstPartProxy &src,
	                                   const Goal::Slice::ConstPartProxy &dst) const;

	static int evalSliceRedundancyLevel(const Goal::Slice &slice);
	bool removePartBasicTest(const Goal::Slice::Type &slice_type, bool &result) const;
	void evalOperationCount();
	int evalRedundancyLevel(const Goal &goal);

protected:
	Goal available_; /*!< Goal describing available chunk parts. */
	Goal target_;    /*!< Goal describing desired chunk parts */

	int target_redundancy_level_;                 /*!< Number of redundant chunk parts for
	                                                   target. */
	int redundancy_level_;                        /*!< Number of redundant chunk parts for
	                                                   whole chunk. */
	SliceStateContainer slice_redundancy_level_;  /*!< Number of redundant chunk parts for
	                                                   each available slice. */
	SliceOpCountContainer slice_operation_count_; /*!< Operation count for each slice */
	std::pair<int, int> operation_count_;         /*!< Operation count for whole chunk. */
};
