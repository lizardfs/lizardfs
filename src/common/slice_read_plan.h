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
#include <cstring>

#include "common/read_plan.h"
#include "common/slice_traits.h"
#include "common/small_vector.h"

/*! \brief Class handling read operations on a single slice.
 * \see ReadPlan
 */
class SliceReadPlan : public ReadPlan {
public:
	struct RequestedPartInfo {
		int part;
		int size;
	};

	SliceReadPlan(Goal::Slice::Type type)
	: ReadPlan(), slice_type(type), buffer_part_size() {}

	/*!
	 * \return true iff reading operation is finished, given available parts
	 * Assumption: available parts are unique.
	 */
	bool isReadingFinished(const PartsContainer &available_parts) const override {
		if (available_parts.size() >= (size_t)slice_traits::requiredPartsToRecover(slice_type)) {
			return true;
		}

		// Count occurrences
		std::array<int, Goal::Slice::kMaxPartsCount> part_counter{{0}};
		for (const auto &part : available_parts) {
			assert(part.getSliceType() == slice_type);
			part_counter[part.getSlicePart()]++;
		}

		bool can_read = std::all_of(requested_parts.begin(), requested_parts.end(),
				[&part_counter](const RequestedPartInfo &info) {
			return part_counter[info.part] > 0;
		});

		return can_read;
	}

	/*!
	 * \return true, iff there is still hope of finishing with a success,
	 * knowing that unreadable_parts cannot be read
	 */
	bool isFinishingPossible(const PartsContainer &unreadable_parts) const override {
		if (read_operations.size() - unreadable_parts.size() >= (size_t)slice_traits::requiredPartsToRecover(slice_type)) {
			return true;
		}
		// Count occurrences
		std::array<int, Goal::Slice::kMaxPartsCount> part_counter{{0}};
		for (const auto &part : unreadable_parts) {
			assert(part.getSliceType() == slice_type);
			part_counter[part.getSlicePart()]++;
		}

		bool can_finish = !std::any_of(requested_parts.begin(), requested_parts.end(),
				[&part_counter](const RequestedPartInfo &info) {
			return part_counter[info.part] > 0;
		});

		return can_finish;
	}

	/*!
	 * Aligns the parts with trailing zeros.
	 * \return size of the output buffer
	 */
	int postProcessRead(uint8_t *buffer, const PartsContainer &) const override {
		int part_offset = 0;
		for(const auto &info : requested_parts) {
			assert(info.size <= buffer_part_size);
			assert((buffer + part_offset + info.size) >= buffer_read);
			assert((buffer + part_offset + buffer_part_size) <= buffer_end);
			std::memset(buffer + part_offset + info.size, 0, buffer_part_size - info.size);
			part_offset += buffer_part_size;
		}

		return requested_parts.size() * buffer_part_size;
	}

public:
	Goal::Slice::Type slice_type;
	small_vector<RequestedPartInfo, 32> requested_parts;
	int buffer_part_size;
};

inline std::string to_string(const SliceReadPlan& plan) {
	std::string result;
	result = to_string(static_cast<const ReadPlan&>(plan));
	result += ":(";
	for (const auto &part : plan.requested_parts) {
		result += std::to_string(part.part) + ",";
	}
	result += ")";

	return result;
}
