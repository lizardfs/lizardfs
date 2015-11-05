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

#include "common/exceptions.h"
#include "common/linear_assignment_optimizer.h"
#include "common/massert.h"

const int Goal::Slice::Type::kTypeParts[Goal::Slice::Type::kTypeCount] = {
	1, 1, 3, 4,  5, 6, 7, 8, 9, 10};

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
	int ret = 0;
	for (const auto &labels : data_) {
		ret += countLabels(labels);
	}
	return ret;
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

	for (int i = 0; i < size(); i++) {
		for (int j = 0; j < size(); j++) {
			auto tmp_union = getLabelsUnion(data_[i], other.data_[j]);
			cost[i][j] = 10 * Goal::kMaxExpectedCopies - labelsDistance(data_[i], tmp_union);
		}
	}

	linear_assignment::auctionOptimization(cost, assignment, size());

	for (int i = 0; i < size(); i++) {
		auto tmp_union = getLabelsUnion(data_[i], other.data_[assignment[i]]);
		data_[i] = std::move(tmp_union);
	}
}

/*! \brief Slice is valid if size is as expected and there is at least one label for each part. */
bool Goal::Slice::isValid() const {
	if (!type_.isValid() || data_.size() != (std::size_t)type_.expectedParts()) {
		return false;
	}
	for (const auto &labels : data_) {
		if (!std::any_of(labels.begin(), labels.end(),
				[](const std::pair<MediaLabel, int> &label) {
					return label.second > 0;
				})) {
			return false;
		}
	}
	return true;
}

Goal::Slice::Labels Goal::Slice::getLabelsUnion(const Labels &first, const Labels &second) {
	Labels merged_labels;

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

		while (second_label_it != second.end() &&
		       first_label.first > second_label_it->first) {
			assert(second_label_it->first != MediaLabel::kWildcard);
			merged_labels.insert(merged_labels.end(), *second_label_it);
			merged_sum += second_label_it->second;
			second_sum += second_label_it->second;
			++second_label_it;
		}
		if (second_label_it == second.end() || first_label.first < second_label_it->first) {
			merged_labels.insert(merged_labels.end(), first_label);
			first_sum += first_label.second;
			merged_sum += first_label.second;
		} else {
			merged_labels.insert(
			        merged_labels.end(), {first_label.first,
			        std::max(first_label.second, second_label_it->second)});
			first_sum += first_label.second;
			second_sum += second_label_it->second;
			merged_sum += std::max(first_label.second, second_label_it->second);
			++second_label_it;
		}
	}
	while (second_label_it != second.end()) {
		if (second_label_it->first != MediaLabel::kWildcard) {
			merged_labels.insert(merged_labels.end(), *second_label_it);
			merged_sum += second_label_it->second;
		}
		second_sum += second_label_it->second;
		++second_label_it;
	}

	int wildcards = std::max(first_sum, second_sum) - merged_sum;
	if (wildcards > 0) {
		merged_labels[MediaLabel::kWildcard] = wildcards;
	}

	return merged_labels;
}

/*! \brief First norm of 'vectors' difference */
int Goal::Slice::labelsDistance(const Labels &first, const Labels &second) {
	int ret = 0;
	auto second_label_it = second.begin();
	for (const auto &first_label : first) {
		while (second_label_it != second.end() &&
		       first_label.first > second_label_it->first) {
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
void Goal::mergeIn(const Goal &otherGoal) {
	for (const auto &other_slice : otherGoal.goal_slices_) {
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
