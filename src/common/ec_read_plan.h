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
#include "common/reed_solomon.h"
#include "common/slice_read_plan.h"

/*!
 * Class handling read operations on a single erasure code slice.
 */
class ECReadPlan : public SliceReadPlan {
public:
	/*!
	 * Computes one parity part from continuous chunk data.
	 */
	struct RecoverParity {
		void operator()(uint8_t *dst, int, const uint8_t *src, int) const {
			typedef ReedSolomon<slice_traits::ec::kMaxDataCount, slice_traits::ec::kMaxParityCount>
			    RS;
			RS rs(data_part_count, parity_part_count);
			RS::ErasedMap erased;
			RS::ConstFragmentMap data_parts{{0}};
			RS::FragmentMap result_parts{{0}};

			assert(plan);

			for (int i = 0; i < parity_part_count; ++i) {
				erased.set(data_part_count + i);
			}

			for (int block = 0; block < part_block_count; ++block) {
				assert(dst >= plan->buffer_start && (dst + MFSBLOCKSIZE) <= plan->buffer_read);

				result_parts[data_part_count + parity_part_index] = dst;
				for (int i = 0; i < data_part_count; ++i) {
					assert(src >= plan->buffer_start && (src + MFSBLOCKSIZE) <= plan->buffer_end);
					data_parts[i] = src;
					src += MFSBLOCKSIZE;
				}

				rs.recover(data_parts, erased, result_parts, MFSBLOCKSIZE);
				dst += MFSBLOCKSIZE;
			}
		}

		int data_part_count; /*!< Number of data parts for Reed-Solomon erasure code. */
		int parity_part_count; /*!< Number of parity parts for Reed-Solomon erasure code. */
		int parity_part_index; /*!< Index of parity part to recover (starting from 0). */
		int part_block_count; /*!< Number of blocks to compute. */

#ifndef NDEBUG
		ReadPlan *plan;
#endif
	};

	ECReadPlan(Goal::Slice::Type type) : SliceReadPlan(type) {
		assert(slice_traits::isEC(type));
	}

	/*! Checks if any part needs to be recovered and, if so, performs the recovery.
	 *
	 * \param buffer
	 * \param available_parts
	 * \return size of post-processed data (in bytes)
	 */
	int postProcessRead(uint8_t *buffer, const PartsContainer &available_parts) const override {
		SliceReadPlan::postProcessRead(buffer, available_parts);

		std::bitset<Goal::Slice::kMaxPartsCount> available_map;
		for (const auto &part : available_parts) {
			assert(part.getSliceType() == slice_type);
			available_map.set(part.getSlicePart());
		}

		if (std::any_of(requested_parts.begin(), requested_parts.end(),
		                [&available_map](const RequestedPartInfo &info) {
			                return !available_map[info.part];
			            })) {
			recoverParts(buffer, available_map);
		}

		return requested_parts.size() * buffer_part_size;
	}

protected:
	/*! \brief Recover missing parts using Reed-Solomon decoder.
	 *
	 * \param buffer Pointer to buffer with data.
	 * \param available_parts Bit-set with information about available parts.
	 */
	void recoverParts(uint8_t *buffer,
	                  const std::bitset<Goal::Slice::kMaxPartsCount> &available_parts) const {
		typedef ReedSolomon<slice_traits::ec::kMaxDataCount, slice_traits::ec::kMaxParityCount> RS;

		int k = slice_traits::ec::getNumberOfDataParts(slice_type);
		int m = slice_traits::ec::getNumberOfParityParts(slice_type);
		int max_parts = k + m;

		RS::ConstFragmentMap data_parts{{0}};
		RS::FragmentMap result_parts{{0}};
		RS::ErasedMap erased;
		RS rs(k, m);

		int available_count = 0;
		for (int i = 0; i < max_parts; ++i) {
			if (!available_parts[i] || available_count >= k) {
				erased.set(i);
			} else {
				available_count++;
			}
		}

		for (auto const& op : read_operations) {
			data_parts[op.first.getSlicePart()] = buffer + op.second.buffer_offset;
		}

		for (int i = 0; i < (int)requested_parts.size(); ++i) {
			if (!available_parts[requested_parts[i].part]) {
				result_parts[requested_parts[i].part] = buffer + i * buffer_part_size;
			}
		}

		rs.recover(data_parts, erased, result_parts, buffer_part_size);
	}
};
