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

#include "common/block_xor.h"
#include "common/read_plan.h"
#include "common/slice_read_plan.h"

/*!
 * Class handling read operations on a single xor slice.
 */
class XorReadPlan : public SliceReadPlan {
public:

	/*!
	 * Computes parity from given blocks by accumulating xored blocks in memory pointed by dst
	 */
	struct RecoverParity {
		void operator()(uint8_t *dst, int, const uint8_t *src, int) const {
			assert(plan);
			for (int block = 0; block < part_block_count; ++block) {
				assert(dst >= plan->buffer_start && (dst + MFSBLOCKSIZE) <= plan->buffer_read);
				std::memcpy(dst, src, MFSBLOCKSIZE);
				src += MFSBLOCKSIZE;
				for (int i = 1; i < data_part_count; ++i) {
					assert(dst >= plan->buffer_start && (dst + MFSBLOCKSIZE) <= plan->buffer_read);
					assert(src >= plan->buffer_start && (src + MFSBLOCKSIZE) <= plan->buffer_end);
					blockXor(dst, src, MFSBLOCKSIZE);
					src += MFSBLOCKSIZE;
				}
				dst += MFSBLOCKSIZE;
			}
		}

		int data_part_count;
		int part_block_count;

#ifndef NDEBUG
		ReadPlan *plan;
#endif
	};

	XorReadPlan(Goal::Slice::Type type) : SliceReadPlan(type) {
	}

	/*!
	 * Checks if any part needs to be recovered from parity and, if so,
	 * performs the recovery.
	 * Firstly, missing part is located. If it cannot be found - no recovery is needed.
	 * Otherwise, missing part is created by applying a xor operation with all existing parts.
	 *
	 * \param buffer
	 * \param available_parts
	 * \return size of post-processed data (in bytes)
	 */
	int postProcessRead(uint8_t *buffer, const PartsContainer &available_parts) const override {
		SliceReadPlan::postProcessRead(buffer, available_parts);

		// Count occurrences
		std::bitset<Goal::Slice::kMaxPartsCount> part_bitset;
		for (const auto &part : available_parts) {
			assert(part.getSliceType() == slice_type);
			part_bitset.set(part.getSlicePart());
		}

		auto missing_it = std::find_if(requested_parts.begin(), requested_parts.end(),
			[&part_bitset](const RequestedPartInfo &info) {
				return part_bitset.test(info.part) == 0;
			});

		// All parts were read - return
		if (missing_it == requested_parts.end()) {
			return requested_parts.size() * buffer_part_size;
		}

		int missing_offset = std::distance(requested_parts.begin(), missing_it) * buffer_part_size;
		int missing_size = missing_it->size;
		bool first = true;

		for (const auto &op : read_operations) {
			if (part_bitset.test(op.first.getSlicePart()) == 0) {
				continue;
			}
			int size = std::min(op.second.request_size, missing_size);
			if (first) {
				assert((buffer + missing_offset) >= buffer_read &&
				       (buffer + missing_offset + size) <= buffer_end);
				assert((buffer + op.second.buffer_offset) >= buffer_read &&
				       (buffer + op.second.buffer_offset + size) <= buffer_end);
				assert((buffer + missing_offset + size) >= buffer_read &&
				       (buffer + missing_offset + missing_size) <= buffer_end);
				std::memcpy(buffer + missing_offset, buffer + op.second.buffer_offset, size);
				std::memset(buffer + missing_offset + size, 0, missing_size - size);
				first = false;
			} else {
				assert((buffer + missing_offset) >= buffer_read &&
				       (buffer + missing_offset + size) <= buffer_end);
				assert((buffer + op.second.buffer_offset) >= buffer_read &&
				       (buffer + op.second.buffer_offset + size) <= buffer_end);
				blockXor(buffer + missing_offset, buffer + op.second.buffer_offset, size);
			}
		}

		return requested_parts.size() * buffer_part_size;
	}
};
