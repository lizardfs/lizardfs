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
#include <map>
#include <memory>
#include <vector>
#include <set>

#include "common/chunk_part_type.h"
#include "common/small_vector.h"

/*! \brief Base ReadPlan class.
 *
 * This class stores data required to handle complex read operation.
 *
 * Let's analyze typical read operation for ec(3,2) (erasure code with 3 data parts
 * and 2 parity parts). We have all chunk parts available.
 *
 * One possible read plan for this situation could look like this:
 *
 * wave 0: read part 1, read part 2, read part 3
 * wave 1: read part 4, read part 5
 *
 * In the first wave we start operation for all data parts. Unfortunately, chunkserver with
 * part 1 cannot send requested part. We reach wave timeout with only two parts.
 * At this moment we start executing reads from next wave. After some time we receive
 * part 5. The isReadingFinished function returns true because we are able to recover
 * data part 3 from available data and parity parts. The postProcessData function computes
 * missing data and operation is finished.
 *
 * With the ReadPlan, much more complicated scenarios with
 * recovery and multiple conversions in the same read operations are possible.
 */
class ReadPlan {
public:
	typedef small_vector<ChunkPartType, Goal::Slice::kMaxPartsCount/2> PartsContainer;

	struct ReadOperation {
		int request_offset; /*!< Offset to be sent in read request. */
		int request_size; /*!< Size to be sent in read request (can be 0). */
		int buffer_offset; /*!< Offset in read buffer (starting from 0). */
		int wave; /*!< Index of wave starting read request (first wave has index 0). */
	};

	/*! \brief Default constructor. */
	ReadPlan() : read_buffer_size(0), block_prefetch(false) {
	}

	/*! \brief Virtual destructor. */
	virtual ~ReadPlan() {
	}

	/*! \brief Function returns offset of the read buffer. */
	int readOffset() const {
		return std::accumulate(
		    postprocess_operations.begin(), postprocess_operations.end(), 0,
		    [](int sum,
		       const std::pair<int, std::function<void(uint8_t *, int, uint8_t *, int)>> &elem) {
			    return sum + elem.first;
			});
	}

	/*! \brief Function returns size of read destination buffer. */
	int readBufferSize() const {
		return read_buffer_size;
	}

	/*! \brief Function returns size of full buffer (post process + read). */
	int fullBufferSize() const {
		return read_buffer_size + readOffset();
	}

	/*! \brief Check if we have enough parts to fulfill read request.
	 *
	 * \param available_parts Container with types of available parts.
	 * \return true if read request can be fulfilled (note: it may still require post-processing
	 *              to compute missing parts).
	 *         false not enough parts to finish request
	 */
	virtual bool isReadingFinished(const PartsContainer &available_parts) const = 0;

	/*! \brief Check if is possible to fulfill read request even when some parts cannot be read.
	 *
	 * \param unreadable_parts Container with types of parts that couldn't be read
	 *                         and won't be available.
	 * \return true if it is possible to finish read request (it may require starting new read
	 *              operations in next waves).
	 *         false it is not possible to read (or recover) requested data.
	 */
	virtual bool isFinishingPossible(const PartsContainer &unreadable_parts) const = 0;

	/*! \brief Post-process data only in read buffer.
	 *
	 * \param buffer Pointer to read buffer.
	 * \param available_parts Container with types of available parts.
	 * \return Size of post-processed data (in bytes).
	 */
	virtual int postProcessRead(uint8_t *buffer, const PartsContainer &available_parts) const = 0;

	/*! \brief Execute all post-process operations.
	 *
	 * Full buffer layout (n - number of post-process operations):
	 *
	 * [post_process[n-1].first bytes] - nth post-process dest buffer    --
	 * ...                                                                |
	 * ...                                                                > full buffer
	 * [post_process[0].first bytes]   - first post-process dest buffer   |
	 * [read_buffer_size bytes]        - read buffer                     --
	 *
	 * The function executes postProcessRead on read buffer first.
	 * Then it executes each post-process operation taking previous buffer as input
	 * and storing result in next post-process buffer.
	 *
	 * This backward to front processing allows for simple buffer resizing
	 * to get rid of unnecessary temporary data.
	 *
	 * \param buffer Pointer to full buffer.
	 * \param available_parts Container with types of available parts.
	 * \return Size of post-processed data (in bytes).
	 */
	virtual int postProcessData(uint8_t *buffer,
	                            const PartsContainer &available_parts) const {
		uint8_t *read_buffer = buffer + readOffset();

		int size = postProcessRead(read_buffer, available_parts);
		assert(size > 0);

		for (const auto &postprocess : postprocess_operations) {
			assert(size >= 0 && postprocess.first >= 0);
			assert((read_buffer - postprocess.first) >= buffer_start && read_buffer <= buffer_read);
			assert(read_buffer + size <= buffer_end);
			postprocess.second(read_buffer - postprocess.first, size, read_buffer,
			                   postprocess.first > 0 ? postprocess.first : size);
			read_buffer -= postprocess.first;
			size = postprocess.first > 0 ? postprocess.first : size;
		}
		assert(read_buffer == buffer);

		return size;
	}


	int read_buffer_size; /*!< Size of read buffer. */

	bool block_prefetch; /*!< True when prefetch requests should be sent to chunkservers. */

	/*! \brief List of read operation to execute for chunk parts. */
	small_vector<std::pair<ChunkPartType, ReadOperation>, Goal::Slice::kMaxPartsCount/2> read_operations;

	/*! \brief List of post-process operations.
	 *
	 * Each entry in the list is a pair containing size of output buffer and a function
	 * doing the post-process operation. The function has following definition:
	 *
	 * void post_process(uint8_t *destination_buffer, int destination_size,
	 *                   const uint8_t* source_buffer, int source_size)
	 *
	 * \param destination_buffer Address of output buffer (where post-processed data should be stored).
	 * \param destination_size Size of output buffer.
	 * \param source_buffer Address of input buffer containing data to be processed.
	 * \param source_size Size of input buffer.
	 */
	small_vector<std::pair<int, std::function<void(uint8_t *, int, const uint8_t *, int)>>, 3>
	    postprocess_operations;

#ifndef NDEBUG
	uint8_t *buffer_start;
	uint8_t *buffer_read;
	uint8_t *buffer_end;
#endif
};

inline std::string to_string(const ReadPlan& plan) {
	std::string result;
	for(const auto &op : plan.read_operations) {
		result += std::to_string(op.second.wave) + ":([" + to_string(op.first) + "]:" + std::to_string(op.second.request_offset)
			+ ":" + std::to_string(op.second.request_size)
			+ ":" + std::to_string(op.second.buffer_offset) + "),";
	}
	return result;
}
