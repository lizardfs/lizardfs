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

#include "common/platform.h"
#include "unittests/plan_tester.h"

#include <algorithm>
#include <iostream>

#include "common/block_xor.h"
#include "common/massert.h"
#include "common/reed_solomon.h"
#include "common/slice_traits.h"

namespace unittests {

/*! \brief Simulate read from chunkserver.
 * \param output Vector for storing data from simulated chunkserver.
 * \param output_offset Offset where data should be stored in output vector.
 * \param data Vector with chunkserver data.
 * \param offset Offset with position of data in \param data vector that should be read.
 * \param size Size of data to read.
 * \return 0 on succes
 *         -1 on failure
 */
int ReadPlanTester::readDataFromChunkServer(std::vector<uint8_t> &output, int output_offset,
		const std::vector<uint8_t> &data, int offset, int size) {
	for (int i = 0; i < size; ++i) {
		assert(output_offset < (int)output.size());
		if (offset >= MFSCHUNKSIZE) {
			return -1;
		}

		output[output_offset++] = offset < (int)data.size() ? data[offset++] : 0;
	}

	return 0;
}


/*! \brief Start read operation.
 *
 * \param write_buffer_offset Offset where data should be stored in output_buffer_ vector.
 * \param available_data Map with data for each chunk part type.
 * \param op Structure describing requested read operation.
 * \return 0 on succes
 *         -1 on failure
 */
int ReadPlanTester::startReadOperation(int write_buffer_offset,
		const std::map<ChunkPartType, std::vector<uint8_t>> &available_data,
		ChunkPartType chunk_type, const ReadPlan::ReadOperation &op) {
	assert(std::find(networking_failures_.begin(), networking_failures_.end(), chunk_type) ==
	       networking_failures_.end());

	auto it = available_data.find(chunk_type);
	if (it == available_data.end()) {
		return -1;
	}

	return readDataFromChunkServer(output_buffer_, write_buffer_offset + op.buffer_offset,
	                               it->second, op.request_offset, op.request_size);
}

/*! \brief Start reads for specified wave. */
void ReadPlanTester::startReadsForWave(
		const std::unique_ptr<ReadPlan> &plan,
		const std::map<ChunkPartType, std::vector<uint8_t>> &available_data, int wave) {
	int write_buffer_offset = plan->readOffset();

	for (const auto &read_operation : plan->read_operations) {
		if (read_operation.second.wave == wave) {
			if (startReadOperation(write_buffer_offset, available_data, read_operation.first,
			                       read_operation.second) < 0) {
				networking_failures_.push_back(read_operation.first);
			} else {
				available_parts_.push_back(read_operation.first);
			}
		}
	}
}

/*! \brief Debug function for testing if plan is correct. */
void ReadPlanTester::checkPlan(const std::unique_ptr<ReadPlan> &plan, uint8_t *buffer_start) {
	(void)plan;
	(void)buffer_start;
#ifndef NDEBUG
	for (const auto &type_and_op : plan->read_operations) {
		assert(type_and_op.first.isValid());
		const ReadPlan::ReadOperation &op(type_and_op.second);
		assert(op.request_offset >= 0 && op.request_size >= 0);
		assert((op.request_offset + op.request_size) <= MFSCHUNKSIZE);
		assert(op.buffer_offset >= 0 &&
		       (op.buffer_offset + op.request_size) <= plan->read_buffer_size);

		if (op.request_size <= 0) {
			continue;
		}

		for (const auto &type_and_op2 : plan->read_operations) {
			if (&type_and_op == &type_and_op2) {
				continue;
			}
			const ReadPlan::ReadOperation &op2(type_and_op2.second);
			bool overlap = true;

			if (op2.request_size <= 0) {
				continue;
			}

			if (op.buffer_offset >= op2.buffer_offset &&
			    op.buffer_offset < (op2.buffer_offset + op2.request_size)) {
				assert(!overlap);
			}
			if ((op.buffer_offset + op.request_size - 1) >= op2.buffer_offset &&
			    (op.buffer_offset + op.request_size - 1) <
			        (op2.buffer_offset + op2.request_size)) {
				assert(!overlap);
			}
			if (op.buffer_offset < op2.buffer_offset &&
			    (op.buffer_offset + op.request_size) >= (op2.buffer_offset + op2.request_size)) {
				assert(!overlap);
			}
		}
	}

	int post_size = 0;
	for (const auto &post : plan->postprocess_operations) {
		assert(post.first >= 0);
		post_size += post.first;
	}

	plan->buffer_start = buffer_start;
	plan->buffer_read = buffer_start + plan->readOffset();
	plan->buffer_end = buffer_start + plan->fullBufferSize();

	assert(plan->buffer_read >= plan->buffer_start && plan->buffer_read < plan->buffer_end);
	assert(plan->buffer_start < plan->buffer_end);
#endif
}

int ReadPlanTester::executePlan(
		std::unique_ptr<ReadPlan> plan,
		const std::map<ChunkPartType, std::vector<uint8_t>> &available_data) {
	int wave;

	output_buffer_.resize(plan->fullBufferSize());
	networking_failures_.clear();
	available_parts_.clear();

	checkPlan(plan, output_buffer_.data());

	for (wave = 0; wave < 10; ++wave) {
		startReadsForWave(plan, available_data, wave);

		if (!plan->isFinishingPossible(networking_failures_)) {
			return -1;
		}

		if (plan->isReadingFinished(available_parts_)) {
			break;
		}
	}

	int result_size = plan->postProcessData(output_buffer_.data(), available_parts_);
	output_buffer_.resize(result_size);

	return wave;
}

/*! \brief Build data for xor chunk part type. */
void ReadPlanTester::buildXorData(std::map<ChunkPartType, std::vector<uint8_t>> &result,
		int level) {
	std::vector<uint8_t> buffer;

	for (int part = 1; part <= level; ++part) {
		if (result.count(slice_traits::xors::ChunkPartType(level, part))) {
			continue;
		}

		int block_count = slice_traits::getNumberOfBlocks(
		    slice_traits::xors::ChunkPartType(level, part), MFSBLOCKSINCHUNK);

		buffer.clear();
		for (int block = 0; block < block_count; ++block) {
			for (int offset = 0; offset < MFSBLOCKSIZE; offset += 4) {
				union conv {
					int32_t value;
					uint8_t data[4];
				} c;
				c.value = (block * level + part - 1) * MFSBLOCKSIZE + offset;
				buffer.insert(buffer.end(), c.data, c.data + 4);
			}
		}
		result.insert({slice_traits::xors::ChunkPartType(level, part), std::move(buffer)});
	}

	if (result.count(slice_traits::xors::ChunkPartType(level, 0))) {
		return;
	}

	// compute parity block
	int block_count = slice_traits::getNumberOfBlocks(slice_traits::xors::ChunkPartType(level, 0),
	                                                  MFSBLOCKSINCHUNK);
	buffer.resize(block_count * MFSBLOCKSIZE, 0);
	for (int part = 1; part <= level; ++part) {
		int size =
		    std::min(buffer.size(), result[slice_traits::xors::ChunkPartType(level, part)].size());
		blockXor(buffer.data(), result[slice_traits::xors::ChunkPartType(level, part)].data(),
		         size);
	}
	result.insert(std::make_pair(slice_traits::xors::ChunkPartType(level, 0), std::move(buffer)));
}

/*! \brief Build data for standard chunk part type. */
void ReadPlanTester::buildStdData(std::map<ChunkPartType, std::vector<uint8_t>> &result) {
	std::vector<uint8_t> buffer;

	if (result.count(slice_traits::standard::ChunkPartType())) {
		return;
	}

	for (int block = 0; block < MFSBLOCKSINCHUNK; ++block) {
		for (int offset = 0; offset < MFSBLOCKSIZE; offset += 4) {
			union conv {
				int32_t value;
				uint8_t data[4];
			} c;
			c.value = block * MFSBLOCKSIZE + offset;
			buffer.insert(buffer.end(), c.data, c.data + 4);
		}
	}
	result.insert({slice_traits::standard::ChunkPartType(), std::move(buffer)});
}

/*! \brief Build data for erasure code chunk part type. */
void ReadPlanTester::buildECData(std::map<ChunkPartType, std::vector<uint8_t>> &result, int k,
		int m) {
	std::vector<uint8_t> buffer;

	for (int part = 0; part <= k; ++part) {
		if (result.count(slice_traits::ec::ChunkPartType(k, m, part))) {
			continue;
		}

		int block_count = slice_traits::getNumberOfBlocks(
		    slice_traits::ec::ChunkPartType(k, m, part), MFSBLOCKSINCHUNK);

		buffer.clear();
		for (int block = 0; block < block_count; ++block) {
			for (int offset = 0; offset < MFSBLOCKSIZE; offset += 4) {
				union conv {
					int32_t value;
					uint8_t data[4];
				} c;
				c.value = (block * k + part) * MFSBLOCKSIZE + offset;
				buffer.insert(buffer.end(), c.data, c.data + 4);
			}
		}
		if (block_count <
		    (int)slice_traits::getNumberOfBlocks(slice_traits::ec::ChunkPartType(k, m, 0))) {
			buffer.insert(buffer.end(), MFSBLOCKSIZE, 0);
		}
		result.insert({slice_traits::ec::ChunkPartType(k, m, part), std::move(buffer)});
	}

	int block_count =
	    slice_traits::getNumberOfBlocks(slice_traits::ec::ChunkPartType(k, m, k), MFSBLOCKSINCHUNK);

	typedef ReedSolomon<slice_traits::ec::kMaxDataCount, slice_traits::ec::kMaxParityCount> RS;
	std::vector<std::vector<uint8_t>> parity_buffers;
	RS::ConstFragmentMap data_parts{{}};
	RS::FragmentMap parity_parts{{}};
	RS rs(k, m);

	parity_buffers.resize(m);

	for (int i = 0; i < m; ++i) {
		parity_buffers[i].resize(block_count * MFSBLOCKSIZE, 0);
		parity_parts[i] = parity_buffers[i].data();
	}
	for (int i = 0; i < k; ++i) {
		data_parts[i] = result[slice_traits::ec::ChunkPartType(k, m, i)].data();
	}

	rs.encode(data_parts, parity_parts, block_count * MFSBLOCKSIZE);

	for (int i = 0; i < m; ++i) {
		result[slice_traits::ec::ChunkPartType(k, m, k + i)] = std::move(parity_buffers[i]);
	}
}

bool ReadPlanTester::compareBlocks(const std::vector<uint8_t> &a, int a_offset,
		const std::vector<uint8_t> &b, int b_offset, int block_count) {
	for (int block = 0; block < block_count; ++block) {
		for (int offset = 0; offset < MFSBLOCKSIZE; offset += 4) {
			union conv {
				int32_t value;
				uint8_t data[4];
			} c1, c2;

			for (int k = 0; k < 4; ++k) {
				assert(a_offset < (int)a.size());
				c1.data[k] = a[a_offset++];
				c2.data[k] = b_offset < (int)b.size() ? b[b_offset++] : 0;
			}

			if (c1.value != c2.value) {
				std::cout << "failure at block " << block << " offset " << offset << " ("
				          << c1.value << "," << c2.value << ")\n";
				return false;
			}
		}
	}
	return true;
}

}  // unittests
