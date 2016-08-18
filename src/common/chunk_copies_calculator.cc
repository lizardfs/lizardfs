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

#include "common/platform.h"
#include "common/chunk_copies_calculator.h"

#include <algorithm>
#include <bitset>

#include "common/goal.h"
#include "common/linear_assignment_optimizer.h"
#include "common/slice_traits.h"
#include "common/random.h"

ChunkCopiesCalculator::ChunkCopiesCalculator() :
	target_redundancy_level_(std::numeric_limits<int>::min()),
	redundancy_level_(std::numeric_limits<int>::min()) {
}

ChunkCopiesCalculator::ChunkCopiesCalculator(Goal target) :
	target_(std::move(target)),
	target_redundancy_level_(std::numeric_limits<int>::min()),
	redundancy_level_(std::numeric_limits<int>::min()) {
}

void ChunkCopiesCalculator::setTarget(Goal target) {
	target_ = std::move(target);
	target_redundancy_level_ = std::numeric_limits<int>::min();
}

void ChunkCopiesCalculator::removePart(const Goal::Slice::Type &slice_type, int part,
					const MediaLabel &label) {
	if (available_.find(slice_type) == available_.end()) {
		return;
	}

	auto labels = available_[slice_type][part];

	auto ilabel = labels.find(label);
	if (ilabel == labels.end()) {
		return;
	}

	--(ilabel->second);
	if (ilabel->second <= 0) {
		labels.erase(ilabel);
	}
}

void ChunkCopiesCalculator::optimize() {
	constexpr int max_slice_op = 10 * Goal::kMaxExpectedCopies;

	for (auto &target_slice : target_) {
		auto slice_it = available_.find(target_slice.getType());
		if (slice_it == available_.end() ||
		    target_slice.size() <= 1) {
			continue;
		}
		const Goal::Slice &src_slice = *slice_it;

		std::array<std::array<int, Goal::Slice::kMaxPartsCount>,
		           Goal::Slice::kMaxPartsCount> cost;
		std::array<int, Goal::Slice::kMaxPartsCount> assignment, object_assignment;

		int i = 0;
		for(const auto &src_part : src_slice) {
			int j = 0;
			for(const auto &target_part : static_cast<const Goal::Slice&>(target_slice)) {
				auto op_count = operationCount(src_part, target_part);
				op_count =
				        std::make_pair(std::min(op_count.first, max_slice_op - 1),
				                       std::min(op_count.second, max_slice_op - 1));
				cost[i][j] = max_slice_op * max_slice_op -
				             (max_slice_op * op_count.first + op_count.second);
				++j;
			}
			++i;
		}

		linear_assignment::auctionOptimization(cost, assignment, object_assignment,
		                                       target_slice.size());

		Goal::Slice result(target_slice.getType());
		i = 0;
		for (auto result_part : result) {
			result_part = target_slice[assignment[i]];
			++i;
		}
		target_slice = std::move(result);
	}

	evalOperationCount();
	evalRedundancyLevel();
}

void ChunkCopiesCalculator::evalOperationCount() {
	operation_count_ = {0, 0};

	slice_operation_count_.clear();

	for (const auto &slice : target_) {
		if (available_.find(slice.getType()) == available_.end()) {
			auto &slice_count(slice_operation_count_[slice.getType()]);
			slice_count.resize(slice.size());

			int i = 0;
			for (const auto &slice_part : slice) {
				auto op_count =
				        std::make_pair(Goal::Slice::countLabels(slice_part), 0);
				slice_count[i] = op_count;
				operation_count_.first += op_count.first;
				++i;
			}
			continue;
		}
		Goal::Slice &src_slice(available_[slice.getType()]);

		auto &slice_count(slice_operation_count_[slice.getType()]);

		slice_count.resize(slice.size());

		int i = 0;
		auto src_slice_it = src_slice.cbegin();
		for (const auto &slice_part : slice) {
			auto op_count = operationCount(*src_slice_it, slice_part);
			slice_count[i] = op_count;
			operation_count_.first += op_count.first;
			operation_count_.second += op_count.second;
			++src_slice_it;
			++i;
		}
	}

	for (const auto &slice : available_) {
		if (target_.find(slice.getType()) == target_.end()) {
			auto &slice_count(slice_operation_count_[slice.getType()]);
			slice_count.resize(slice.size());

			int i = 0;
			for (const auto &slice_part : slice) {
				auto op_count =
				        std::make_pair(0, Goal::Slice::countLabels(slice_part));
				slice_count[i] = op_count;
				operation_count_.second += op_count.second;
				++i;
			}
		}
	}
}

std::pair<int, int> ChunkCopiesCalculator::operationCount(const Goal::Slice::ConstPartProxy &src,
							const Goal::Slice::ConstPartProxy &dst) const {
	int wcount = 0; // number of wildcard servers

	auto ilabel = dst.find(MediaLabel::kWildcard);
	if (ilabel != dst.end()) {
		wcount = ilabel->second;
	}

	std::pair<int, int> result;

	// In the loop we use trick to reduce number of
	// chunk parts that we need to count as marked for removal.
	// Instead of counting each extra part we try to consider
	// up to wcount of them as matching wildcard label and remove only remaining.
	auto isrc = src.begin();
	for (const auto &label : dst) {
		if (label.first == MediaLabel::kWildcard) {
			break;
		}

		for (; isrc != src.end() && isrc->first < label.first; ++isrc) {
			result.second += isrc->second - std::min(wcount, (int)isrc->second);
			wcount -= std::min(wcount, (int)isrc->second);
		}

		if (isrc != src.end() && isrc->first == label.first) {
			if (label.second < isrc->second) {
				int remove = isrc->second - label.second;
				result.second += remove - std::min(wcount, remove);
				wcount -= std::min(wcount, remove);
			} else if (label.second > isrc->second) {
				result.first += label.second - isrc->second;
			}
			++isrc;
			continue;
		}
		result.first += label.second;
	}
	for (; isrc != src.end(); ++isrc) {
		result.second += isrc->second - std::min(wcount, (int)isrc->second);
		wcount -= std::min(wcount, (int)isrc->second);
	}
	result.first += wcount;

	return result;
}

void ChunkCopiesCalculator::evalRedundancyLevel() {
	redundancy_level_ = 0;
	for (const auto &slice : available_) {
		int extra_parts = evalSliceRedundancyLevel(slice);
		slice_redundancy_level_[slice.getType()] = extra_parts;
		redundancy_level_ += std::max(extra_parts + 1, 0);
	}
	redundancy_level_ -= 1;
}

int ChunkCopiesCalculator::evalRedundancyLevel(const Goal &goal) {
	int redundancy_level = 0;
	for (const auto &slice : goal) {
		int extra_parts = evalSliceRedundancyLevel(slice);
		redundancy_level += std::max(extra_parts + 1, 0);
	}
	return redundancy_level - 1;
}

// Negative return value indicates that data is lost
int ChunkCopiesCalculator::evalSliceRedundancyLevel(
		const Goal::Slice &slice) {
	int type_count = 0, copies = 0;

	for (const auto &part : slice) {
		int count = Goal::Slice::countLabels(part);
		copies += count;
		type_count += count > 0;
	}
	if (slice_traits::isStandard(slice)) {
		return copies - 1;
	} else if (slice_traits::isXor(slice) || slice_traits::isEC(slice)) {
		return type_count - slice_traits::getNumberOfDataParts(slice);
	}
	return -1;
}

bool ChunkCopiesCalculator::isSafeEnoughToWrite(int min_redundant_parts) {
	if (target_redundancy_level_ == std::numeric_limits<int>::min()) {
		target_redundancy_level_ = evalRedundancyLevel(target_);
	}
	if (redundancy_level_ == std::numeric_limits<int>::min()) {
		evalRedundancyLevel();
	}

	min_redundant_parts = std::min(target_redundancy_level_, min_redundant_parts);

	return redundancy_level_ >= min_redundant_parts;
}

void ChunkCopiesCalculator::updateRedundancyLevel(const Goal::Slice::Type &slice_type) {
	if (available_.find(slice_type) == available_.end()) {
		return;
	}
	int old_redundancy_level_ = slice_redundancy_level_[slice_type];
	slice_redundancy_level_[slice_type] = evalSliceRedundancyLevel(available_[slice_type]);
	redundancy_level_ += std::max(slice_redundancy_level_[slice_type], -1) - std::max(old_redundancy_level_, -1);
}

std::pair<int, int> ChunkCopiesCalculator::countPartsToMove(const Goal::Slice::Type &slice_type,
							int part) const {
	assert(part >= 0 && part < slice_type.expectedParts());

	auto icount = slice_operation_count_.find(slice_type);

	if (icount == slice_operation_count_.end()) {
		return {0, 0};
	}

	return icount->second[part];
}

bool ChunkCopiesCalculator::canRemovePart(const Goal::Slice::Type &slice_type, int part,
					const MediaLabel &label) const {
	bool result;
	if (removePartBasicTest(slice_type, result)) {
		return result;
	}

	Goal::Slice slice(available_[slice_type]);
	auto slice_part = slice[part];

	Goal::Slice::Labels::iterator ilabel = slice_part.find(label);
	if (ilabel != slice_part.end()) {
		--(ilabel->second);
	}

	auto itarget_slice = target_.find(slice_type);
	if (itarget_slice != target_.end()) {
		if (itarget_slice->getExpectedCopies() == 1 && slice.getExpectedCopies() >= 1) {
			return true;
		}
	}

	return evalSliceRedundancyLevel(slice) > 0;
}

/*! \brief Test if removing one chunk part doesn't make whole chunk unsafe.
 *
 * \param slice_type type of slice to check
 * \param result result of the check
 * \return if true then result is valid otherwise more tests should be performed.
 */
bool ChunkCopiesCalculator::removePartBasicTest(const Goal::Slice::Type &slice_type,
						bool &result) const {
	// if target is empty then we assume that chunk is always safe
	if (target_.size() == 0) {
		result = true;
		return true;
	}

	// if chunk isn't safe or there is no chunk part of this type
	// then we by design don't allow to remove anything
	if (redundancy_level_ < 1 ||
	    available_.find(slice_type) == available_.end()) {
		result = false;
		return true;
	}

	std::array<int, 3> state_count{{0}};

	assert(ChunksAvailabilityState::kSafe == 0 && ChunksAvailabilityState::kEndangered == 1 &&
	       ChunksAvailabilityState::kLost == 2);

	ChunksAvailabilityState::State slice_state;
	for (const auto &state : slice_redundancy_level_) {
		if (state.second > 0) {
			slice_state = ChunksAvailabilityState::kSafe;
		} else if (state.second == 0) {
			slice_state = ChunksAvailabilityState::kEndangered;
		} else {
			slice_state = ChunksAvailabilityState::kLost;
		}
		state_count[slice_state]++;
	}

	int can_remove_count = 2 * state_count[ChunksAvailabilityState::kSafe] +
	                       state_count[ChunksAvailabilityState::kEndangered];

	// for this to be true we have more than one safe slice
	// or more than two endangered slices
	// or we have one safe and at least one endangered.
	// In each of this situations we can remove any chunk part and whole chunk will be safe.
	if (can_remove_count > 2) {
		result = true;
		return true;
	}

	// at this point we know that can_remove_count == 2

	SliceStateContainer::const_iterator istate = slice_redundancy_level_.find(slice_type);
	assert(istate != slice_redundancy_level_.end());

	// if the slice is lost and chunk is safe then again we can remove it
	// because this slice doesn't impact chunk safety.
	if (istate->second < 0) {
		result = true;
		return true;
	}

	// if we have two endangered slices then we can't remove anything
	if (state_count[ChunksAvailabilityState::kSafe] != 1) {
		result = false;
		return true;
	}

	// at this point we know that we remove from safe slice
	// and we need more tests to check if it won't make it unsafe.
	return false;
}

bool ChunkCopiesCalculator::canMovePartToDifferentLabel(const Goal::Slice::Type &slice_type,
						int part, const MediaLabel &label) const {
	if (available_.find(slice_type) == available_.end() ||
	    target_.find(slice_type) == target_.end()) {
		return false;
	}

	auto target_labels = target_[slice_type][part];
	auto itarget = target_labels.find(label);
	if (itarget == target_labels.end()) {
		return true;
	}

	auto avail_labels = available_[slice_type][part];
	auto iavail = avail_labels.find(label);
	if (iavail == avail_labels.end()) {
		return false;
	}

	return (iavail->second - itarget->second) > 0;
}

Goal::Slice::Labels ChunkCopiesCalculator::getLabelsToRecover(const Goal::Slice::Type &slice_type,
								int part) const {
	assert(part >= 0 && part < slice_type.expectedParts());

	if (target_.find(slice_type) == target_.end()) {
		return Goal::Slice::Labels();
	}
	if (available_.find(slice_type) == available_.end()) {
		auto data = target_[slice_type][part];
		return Goal::Slice::Labels(data.begin(), data.end());
	}

	auto available_labels = available_[slice_type][part];
	auto target_labels = target_[slice_type][part];
	Goal::Slice::Labels result;
	int wcount = 0;

	auto ilabel = target_labels.find(MediaLabel::kWildcard);
	if (ilabel != target_labels.end()) {
		wcount = ilabel->second;
	}

	ilabel = available_labels.begin();
	for (const auto &label : target_labels) {
		if (label.first == MediaLabel::kWildcard) {
			break;
		}

		for (; ilabel != available_labels.end() && ilabel->first < label.first; ++ilabel) {
			wcount -= std::min(wcount, (int)ilabel->second);
		}

		if (ilabel != available_labels.end() && ilabel->first == label.first) {
			if (label.second > ilabel->second) {
				result[label.first] = label.second - ilabel->second;
			}
			if (label.second < ilabel->second) {
				int remove = ilabel->second - label.second;
				wcount -= std::min(wcount, remove);
			}
			++ilabel;
			continue;
		}

		assert(label.second > 0);
		result[label.first] = label.second;
	}
	for (; ilabel != available_labels.end(); ++ilabel) {
		wcount -= std::min(wcount, (int)ilabel->second);
	}

	if (wcount > 0) {
		result[MediaLabel::kWildcard] = wcount;
	}

	return result;
}

Goal::Slice::Labels ChunkCopiesCalculator::getRemovePool(const Goal::Slice::Type &slice_type,
							int part) const {
	assert(part >= 0 && part < slice_type.expectedParts());

	if (available_.find(slice_type) == available_.end()) {
		return Goal::Slice::Labels();
	}
	if (target_.find(slice_type) == target_.end()) {
		auto data = available_[slice_type][part];
		return Goal::Slice::Labels(data.begin(), data.end());
	}

	auto available_labels = available_[slice_type][part];
	auto target_labels = target_[slice_type][part];
	Goal::Slice::Labels result;
	int wcount = 0, wilcard_target_count = 0;

	auto ilabel = available_labels.begin();
	for (const auto &label : target_labels) {
		if (label.first == MediaLabel::kWildcard) {
			wilcard_target_count = label.second;
			break;
		}
		for (; ilabel != available_labels.end() && ilabel->first < label.first; ++ilabel) {
			assert(ilabel->second > 0);
			result[ilabel->first] = 1;
			wcount += ilabel->second;
		}
		if (ilabel != available_labels.end() && ilabel->first == label.first) {
			if (label.second < ilabel->second) {
				int remove = ilabel->second - label.second;
				result[label.first] = 1;
				wcount += remove;
			}
			++ilabel;
		}
	}

	for (; ilabel != available_labels.end(); ++ilabel) {
		assert(ilabel->second > 0);
		result[ilabel->first] = 1;
		wcount += ilabel->second;
	}

	if (wcount <= wilcard_target_count) {
		result.clear();
	}

	return result;
}

int ChunkCopiesCalculator::getFullCopiesCount(const Goal& goal) {
	int count = 0;
	for (const auto &slice : goal) {
		if (slice_traits::isStandard(slice)) {
			count += slice.getExpectedCopies();
		} else if (slice_traits::isXor(slice)) {
			int l1 = std::numeric_limits<int>::max(); // lowest number of copies
			int l2 = std::numeric_limits<int>::max(); // second lowest value of
			for (const auto &part : slice) {
				int part_copies = Goal::Slice::countLabels(part);

				if (part_copies < l1) {
					l2 = l1;
					l1 = part_copies;
				} else {
					l2 = std::min(l2, part_copies);
				}
			}

			// if slice is in lost state then l2 == 0
			count += l2;
		} else if (slice_traits::isEC(slice)) {
			std::array<int, Goal::Slice::kMaxPartsCount> part_copies;

			int i = 0;
			for (const auto &part : slice) {
				part_copies[i++] = Goal::Slice::countLabels(part);
			}

			int data_part_count = slice_traits::getNumberOfDataParts(slice);

			std::nth_element(part_copies.begin(), part_copies.begin() + (data_part_count - 1),
			                 part_copies.begin() + i, [](int a, int b) { return a > b; });

			// if slice is in lost state then part_copies[data_part_count - 1] == 0
			count += part_copies[data_part_count - 1];
		}
	}

	return count;
}
