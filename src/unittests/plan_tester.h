/*
   Copyright 2013-2016 Skytechnology sp. z o.o.

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

#include <cstdint>
#include <ostream>
#include <set>

#include "common/read_plan.h"
#include "common/slice_traits.h"

namespace unittests {

/*! \brief Class designed for testing read plan. */
class ReadPlanTester {
public:
	/*! \brief Execute read plan.
	 *
	 * \param plan Pointer to read plan.
	 * \param available_data Map with data vector for each chunk type
	 * \return number of executed waves.
	 */
	int executePlan(std::unique_ptr<ReadPlan> plan,
	                const std::map<ChunkPartType, std::vector<uint8_t>> &available_data);

	/*! \brief Helper function for building chunk data.
	 *
	 * This function creates chunk data split into specified chunk parts.
	 *
	 * Generated chunk's data have following form:
	 *
	 * 00000000 - (uint32_t)0
	 * 04000000 - (uint32_t)4
	 * 08000000 - (uint32_t)8
	 * ...
	 * ...
	 * 3FFFFFFC - (uint32_t)MFSCHUNKSIZE - 4
	 *
	 * \param plan Pointer to read plan.
	 * \param result Output map storing generated chunk part data.
	 * \param available_data Container with chunk part types that should be created.
	 */
	template<class V = std::vector<ChunkPartType>>
	static void buildData(std::map<ChunkPartType, std::vector<uint8_t>> &result,
						  const V &available_parts);

	/*! \brief Helper function for comparing chunk data.
	 * \param a Vector with first data to compare.
	 * \param a_offset Offset in vector \param a where comparison should start.
	 * \param b Second vector with data to compare.
	 * \param b_offset Offset in vector \param b to data that should be compared.
	 * \param block_count number of blocks to compare (each block have MFSBLOCKSIZE bytes)
	 * \return true both vectors are the same.
	 *         false data are not the same.
	 */
	static bool compareBlocks(const std::vector<uint8_t> &a, int a_offset,
	                          const std::vector<uint8_t> &b, int b_offset, int block_count);

protected:
	void checkPlan(const std::unique_ptr<ReadPlan> &plan, uint8_t *buffer_start);

	int readDataFromChunkServer(std::vector<uint8_t> &output, int output_offset,
	                            const std::vector<uint8_t> &data, int offset, int size);

	int startReadOperation(int write_buffer_offset,
	                       const std::map<ChunkPartType, std::vector<uint8_t>> &available_data,
	                       ChunkPartType chunk_type, const ReadPlan::ReadOperation &op);
	void startReadsForWave(const std::unique_ptr<ReadPlan> &plan,
	                       const std::map<ChunkPartType, std::vector<uint8_t>> &available_data,
	                       int wave);

	static void buildXorData(std::map<ChunkPartType, std::vector<uint8_t>> &result, int level);
	static void buildStdData(std::map<ChunkPartType, std::vector<uint8_t>> &result);
	static void buildECData(std::map<ChunkPartType, std::vector<uint8_t>> &result, int k, int m);

public:
	ReadPlan::PartsContainer available_parts_;
	ReadPlan::PartsContainer networking_failures_;
	std::vector<uint8_t> output_buffer_;
};

template <class V>
void ReadPlanTester::buildData(std::map<ChunkPartType, std::vector<uint8_t>> &result,
		const V &available_parts) {
	std::set<Goal::Slice::Type> used_type;

	for (const auto &part : available_parts) {
		used_type.insert(part.getSliceType());
	}

	for (const auto &type : used_type) {
		if (slice_traits::isStandard(type)) {
			buildStdData(result);
		}
		if (slice_traits::isXor(type)) {
			buildXorData(result, slice_traits::xors::getXorLevel(type));
		}
		if (slice_traits::isEC(type)) {
			buildECData(result, slice_traits::ec::getNumberOfDataParts(type),
			            slice_traits::ec::getNumberOfParityParts(type));
		}
	}
}

}  // namespace unittests
