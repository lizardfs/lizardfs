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
#include "common/goal.h"

#include <limits>
#include <iostream>

#include "common/exceptions.h"
#include "common/linear_assignment_optimizer.h"
#include "common/massert.h"

const int Goal::Slice::Type::kTypeParts[Goal::Slice::Type::kTypeCount] = {
	1, 1, 3, 4, 5, 6, 7, 8, 9, 10};

const std::array<std::string, Goal::Slice::Type::kTypeCount> Goal::Slice::Type::kTypeNames = {{
	"std",
	"tape",
	"xor2",
	"xor3",
	"xor4",
	"xor5",
	"xor6",
	"xor7",
	"xor8",
	"xor9"
}};

int Goal::Slice::getExpectedCopies() const {
	return std::accumulate(
	    data_.begin(), data_.end(), 0,
	    [](int value, const DataContainer::value_type &entry) { return value + entry.second; });
}

/*! \brief Merge in another Slice - they have to be of the same type and size
 *
 * This function minimize number of required chunk parts in resultant goal.
 * It is desirable to minimize number of operations required to replicate chunk to
 * suitable chunkservers. Since we do not know what is current state of replication
 * this function actually minimize number of parts in target goal
 * but number of required operation to achieve the goal may not be optimal.
 */
void Goal::Slice::mergeIn(const Slice &other) {
	assert(type_ == other.type_);
	assert(size() == other.size());

	std::array<std::array<int, kMaxPartsCount>, kMaxPartsCount> cost;
	std::array<int, kMaxPartsCount> assignment;

	Labels tmp_union;

	int i = 0;
	for (const auto &local_part : static_cast<const Goal::Slice&>(*this)) {
		int j = 0;
		for (const auto &other_part : other) {
			makeLabelsUnion(tmp_union, local_part, other_part);
			cost[i][j] = 10 * Goal::kMaxExpectedCopies - labelsDistance(local_part, tmp_union);
			++j;
		}
		++i;
	}

	linear_assignment::auctionOptimization(cost, assignment, size());

	DataContainer result;
	SizeContainer result_size;

	i = 0;
	for (const auto &local_part : static_cast<const Goal::Slice&>(*this)) {
		makeLabelsUnion(tmp_union, local_part, other[assignment[i]]);
		result.insert(result.end(), tmp_union.begin(), tmp_union.end());
		result_size[i] = tmp_union.size();
		++i;
	}

	data_ = std::move(result);
	part_size_ = std::move(result_size);
}

/*! \brief Slice is valid if size is as expected and there is at least one label for each part. */
bool Goal::Slice::isValid() const noexcept {
	if (!type_.isValid()) {
		return false;
	}
	return std::all_of(data_.begin(), data_.end(), [](const DataContainer::value_type &entry) {
		return entry.second > 0;
	}) && std::all_of(begin(), end(), [](const ConstPartProxy &part) {
		return part.size() > 0;
	});
}

void Goal::Slice::makeLabelsUnion(Labels &result, const ConstPartProxy &first, const ConstPartProxy &second) {
	result.clear();

	int first_sum = 0;
	int second_sum = 0;
	int merged_sum = 0;
	auto second_label_it = second.begin();
	for (const auto &first_label : first) {
		if (first_label.first == MediaLabel::kWildcard) {
			first_sum += first_label.second;
			// because kWildcard is always last we can use break and skip one comparison
			break;
		}

		while (second_label_it != second.end() && first_label.first > second_label_it->first) {
			assert(second_label_it->first != MediaLabel::kWildcard);
			result.insert(result.end(), *second_label_it);
			merged_sum += second_label_it->second;
			second_sum += second_label_it->second;
			++second_label_it;
		}
		if (second_label_it == second.end() || first_label.first < second_label_it->first) {
			result.insert(result.end(), first_label);
			first_sum += first_label.second;
			merged_sum += first_label.second;
		} else {
			result.insert(result.end(), {first_label.first,
			                             std::max(first_label.second, second_label_it->second)});
			first_sum += first_label.second;
			second_sum += second_label_it->second;
			merged_sum += std::max(first_label.second, second_label_it->second);
			++second_label_it;
		}
	}
	while (second_label_it != second.end()) {
		if (second_label_it->first != MediaLabel::kWildcard) {
			result.insert(result.end(), *second_label_it);
			merged_sum += second_label_it->second;
		}
		second_sum += second_label_it->second;
		++second_label_it;
	}

	int wildcards = std::max(first_sum, second_sum) - merged_sum;
	if (wildcards > 0) {
		result[MediaLabel::kWildcard] = wildcards;
	}
}

/*! \brief First norm of 'vectors' difference */
int Goal::Slice::labelsDistance(const ConstPartProxy &first, const Labels &second) noexcept {
	int ret = 0;
	auto second_label_it = second.begin();
	for (const auto &first_label : first) {
		while (second_label_it != second.end() && first_label.first > second_label_it->first) {
			ret += second_label_it->second;
			++second_label_it;
		}
		if (second_label_it == second.end() || first_label.first < second_label_it->first) {
			ret += first_label.second;
		} else {
			ret += std::abs(first_label.second - second_label_it->second);
			++second_label_it;
		}
	}
	while (second_label_it != second.end()) {
		ret += second_label_it->second;
		++second_label_it;
	}

	return ret;
}

/*! \brief Current goal becomes union of itself with otherGoal */
void Goal::mergeIn(const Goal &other_goal) {
	for (const auto &other_slice : other_goal.goal_slices_) {
		assert(other_slice.isValid());

		auto position = find(other_slice.getType());
		// Check if there is no element of this type yet
		if (position == goal_slices_.end()) {
			// Union of A with empty set is A
			goal_slices_.insert(other_slice);
		} else {
			Slice &current_element = *position;
			// There should be same number of parts
			assert(current_element.size() == other_slice.size());
			current_element.mergeIn(other_slice);
		}
	}
}

int Goal::getExpectedCopies() const {
	int ret = 0;
	for (const auto &element : goal_slices_) {
		ret += element.getExpectedCopies();
	}
	return ret;
}

std::string to_string(const Goal::Slice &slice) {
	std::string str;

	str = "$" + to_string(slice.getType()) + " ";

	if (slice.size() > 1) {
		str += "{";
	}
	for (int part = 0; part < slice.size(); ++part) {
		if (part > 0) {
			str += " ";
		}

		int labels_count = Goal::Slice::countLabels(slice[part]);

		if (labels_count != 1) {
			str += "{";
		}

		int count = 0;
		for (const auto &label : slice[part]) {
			for (int i = 0; i < label.second; i++) {
				if (count > 0) {
					str += " ";
				}
				str += static_cast<std::string>(label.first);
				++count;
			}
		}

		if (labels_count != 1) {
			str += "}";
		}
	}
	if (slice.size() > 1) {
		str += "}";
	}

	return str;
}

std::string to_string(const Goal &goal) {
	std::string str;

	str = goal.getName() + ": ";

	int count = 0;
	for (const auto &slice : goal) {
		if (count > 0) {
			str += " | ";
		}
		str += to_string(slice);
		++count;
	}

	return str;
}
