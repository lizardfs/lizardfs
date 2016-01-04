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
#include "common/read_plan_executor.h"

#include <chrono>
#include <map>
#include <set>

#include "common/block_xor.h"
#include "common/chunkserver_stats.h"
#include "common/exceptions.h"
#include "common/lambda_guard.h"
#include "common/lizardfs_version.h"
#include "common/massert.h"
#include "common/mfserr.h"
#include "common/read_operation_executor.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "devtools/request_log.h"
#include "protocol/cltocs.h"

std::atomic<uint64_t> ReadPlanExecutor::executions_total_;
std::atomic<uint64_t> ReadPlanExecutor::executions_with_additional_operations_;
std::atomic<uint64_t> ReadPlanExecutor::executions_finished_by_additional_operations_;

ReadPlanExecutor::ReadPlanExecutor(ChunkserverStats &chunkserver_stats, uint64_t chunk_id,
		uint32_t chunk_version, std::unique_ptr<ReadPlan> plan)
	: stats_(chunkserver_stats),
	  chunk_id_(chunk_id),
	  chunk_version_(chunk_version),
	  plan_(std::move(plan)) {
}

/*! \brief A function which starts single read operation from chunkserver.
 *
 * \param params Execution parameters pack.
 * \param chunk_type Chunk part type to start read read operation for.
 * \param op Structure describing read operation.
 *
 * \return true on success.
 *         false on failure.
 */
bool ReadPlanExecutor::startReadOperation(ExecuteParams &params, ChunkPartType chunk_type,
		const ReadPlan::ReadOperation &op) {
#ifndef NDEBUG
	auto it = std::find(networking_failures_.begin(), networking_failures_.end(), chunk_type);
	assert(it == networking_failures_.end());
	assert(params.locations.count(chunk_type));
#endif

	if (op.request_size <= 0) {
		available_parts_.push_back(chunk_type);
		return true;
	}

	const ChunkTypeWithAddress &ctwa = params.locations.at(chunk_type);
	stats_.registerReadOperation(ctwa.address);

	try {
		Timeout connect_timeout(std::chrono::milliseconds(params.connect_timeout));
		int fd = params.connector.startUsingConnection(ctwa.address, connect_timeout);
		try {
			if (params.total_timeout.expired()) {
				// totalTimeout might expire during establishing the connection
				throw RecoverableReadException("Chunkserver communication timed out");
			}
			ReadOperationExecutor executor(op, chunk_id_, chunk_version_, chunk_type, ctwa.address,
			                               ctwa.chunkserver_version, fd, params.buffer);
			executor.sendReadRequest(connect_timeout);
			executors_.insert(std::make_pair(fd, std::move(executor)));
		} catch (...) {
			tcpclose(fd);
			throw;
		}
		return true;
	} catch (ChunkserverConnectionException &ex) {
		last_connection_failure_ = ctwa.address;
		stats_.markDefective(ctwa.address);
		networking_failures_.push_back(chunk_type);
		return false;
	}
}

/*! \brief A function which starts a new prefetch operation.
 *
 * \param params Execution parameters pack.
 * \param chunk_type Chunk part type to start prefetch read operation for.
 * \param op Structure describing prefetch operation.
 */
void ReadPlanExecutor::startPrefetchOperation(ExecuteParams &params, ChunkPartType chunk_type,
		const ReadPlan::ReadOperation &op) {
	assert(params.locations.count(chunk_type));

	if (op.request_size <= 0) {
		return;
	}

	const ChunkTypeWithAddress &ctwa = params.locations.at(chunk_type);

	try {
		Timeout connect_timeout(std::chrono::milliseconds(params.connect_timeout));
		int fd = params.connector.startUsingConnection(ctwa.address, connect_timeout);
		try {
			if (params.total_timeout.expired()) {
				// totalTimeout might expire during establishing the connection
				throw RecoverableReadException("Chunkserver communication timed out");
			}
			std::vector<uint8_t> message;
			if (ctwa.chunkserver_version >= kFirstECVersion) {
				cltocs::prefetch::serialize(message, chunk_id_, chunk_version_, chunk_type,
				                            op.request_offset, op.request_size);
			} else if (ctwa.chunkserver_version >= kFirstXorVersion) {
				assert((int)chunk_type.getSliceType() < Goal::Slice::Type::kECFirst);
				cltocs::prefetch::serialize(message, chunk_id_, chunk_version_,
				                            (legacy::ChunkPartType)chunk_type, op.request_offset,
				                            op.request_size);
			}
			if (message.size() > 0) {
				int32_t ret =
				    tcptowrite(fd, message.data(), message.size(), connect_timeout.remaining_ms());
				if (ret != (int32_t)message.size()) {
					throw ChunkserverConnectionException(
					    "Cannot send PREFETCH request to the chunkserver: " +
					        std::string(strerr(tcpgetlasterror())),
					    ctwa.address);
				}
			}
		} catch (...) {
			tcpclose(fd);
			throw;
		}
		params.connector.endUsingConnection(fd, ctwa.address);
	} catch (ChunkserverConnectionException &ex) {
		// That's a pity
	}
}

/*! \brief A function that starts all read operations for a wave.
 *
 * \param params Execution parameters pack.
 * \param wave Wave index.
 *
 * \return Number of failed read operations.
 */
int ReadPlanExecutor::startReadsForWave(ExecuteParams &params, int wave) {
	int failed_reads = 0;
	for (const auto &read_operation : plan_->read_operations) {
		if (read_operation.second.wave == wave) {
			if (!startReadOperation(params, read_operation.first, read_operation.second)) {
				++failed_reads;
			}
		}
	}
	if (!plan_->isFinishingPossible(networking_failures_)) {
		throw RecoverableReadException("Can't connect to " + last_connection_failure_.toString());
	}

	return failed_reads;
}

/*! \brief A function that starts all prefetch operations for a wave.
 *
 * \param params Execution parameters pack.
 * \param wave Wave index.
 */
void ReadPlanExecutor::startPrefetchForWave(ExecuteParams &params, int wave) {
	if (plan_->disable_prefetch) {
		return;
	}

	for (const auto &prefetch_operation : plan_->read_operations) {
		if (prefetch_operation.second.wave == wave) {
			startPrefetchOperation(params, prefetch_operation.first, prefetch_operation.second);
		}
	}
}

/*! \brief Function waits for data from chunkservers.
 *
 * \param params Execution parameters pack.
 * \param wave_timeout Timeout class keeping time to end of current wave.
 * \param poll_fds Vector with pollfds structures resulting from call to poll system function.
 * \return true on success
 *         false EINTR occured (call to waitForData should be repeated)
 */
bool ReadPlanExecutor::waitForData(ExecuteParams &params, Timeout &wave_timeout,
		std::vector<pollfd> &poll_fds) {
	// Prepare for poll
	poll_fds.clear();
	for (const auto &fd_and_executor : executors_) {
		poll_fds.push_back({fd_and_executor.first, POLLIN, 0});
	}

	if (poll_fds.empty()) {
		return true;
	}

	// Call poll
	int poll_timeout = std::max(
	    0, (int)std::min(params.total_timeout.remaining_ms(), wave_timeout.remaining_ms()));
	int status = tcppoll(poll_fds, poll_timeout);
	if (status < 0) {
#ifdef _WIN32
		throw RecoverableReadException("Poll error: " + std::string(strerr(tcpgetlasterror())));
#else
		if (errno == EINTR) {
			return false;
		} else {
			throw RecoverableReadException("Poll error: " + std::string(strerr(tcpgetlasterror())));
		}
#endif
	}

	return true;
}

/*! \brief Read data from chunkserver.
 *
 * \param params Execution parameters pack.
 * \param poll_fd pollfd structure with the IO events.
 * \param executor Executor for which some data are available.
 */
bool ReadPlanExecutor::readSomeData(ExecuteParams &params, const pollfd &poll_fd,
		ReadOperationExecutor &executor) {
	const NetworkAddress &server = executor.server();

	try {
		if (poll_fd.revents & POLLIN) {
			executor.continueReading();
		} else if (poll_fd.revents & (POLLHUP | POLLERR | POLLNVAL)) {
			throw ChunkserverConnectionException("Read from chunkserver (poll) error", server);
		}
	} catch (ChunkserverConnectionException &ex) {
		stats_.markDefective(server);
		networking_failures_.push_back(executor.chunkType());
		tcpclose(poll_fd.fd);
		executors_.erase(poll_fd.fd);
		if (!plan_->isFinishingPossible(networking_failures_)) {
			throw;
		}
		return false;
	}

	if (executor.isFinished()) {
		stats_.unregisterReadOperation(server);
		stats_.markWorking(server);
		params.connector.endUsingConnection(poll_fd.fd, server);
		available_parts_.push_back(executor.chunkType());
		executors_.erase(poll_fd.fd);
	}

	return true;
}

/*! \brief Execute read operation (without post-process). */
void ReadPlanExecutor::executeReadOperations(ExecuteParams &params) {
	assert(!plan_->read_operations.empty());

	int failed_reads;
	int wave = 0;

	// start reads for first wave (index 0)
	failed_reads = startReadsForWave(params, wave);
	startPrefetchForWave(params, wave + 1);

	assert((executors_.size() + networking_failures_.size()) > 0);

	// Receive responses
	LOG_AVG_TILL_END_OF_SCOPE0("ReadPlanExecutor::executeReadOperations#recv");

	Timeout wave_timeout(std::chrono::milliseconds(params.wave_timeout));
	std::vector<pollfd> poll_fds;

	while (true) {
		if (params.total_timeout.expired()) {
			if (!executors_.empty()) {
				NetworkAddress offender = executors_.begin()->second.server();
				throw RecoverableReadException("Chunkserver communication timed out: " +
				                               offender.toString());
			}
			throw RecoverableReadException("Chunkservers communication timed out");
		}

		if (wave_timeout.expired() || failed_reads) {
			// start next wave
			executions_with_additional_operations_ += wave == 0;
			++wave;
			wave_timeout.reset();
			failed_reads = startReadsForWave(params, wave);
			startPrefetchForWave(params, wave + 1);
		}

		if (!waitForData(params, wave_timeout, poll_fds)) {
			// EINTR occured - we need to restart poll
			continue;
		}

		if (poll_fds.empty()) {
			// no more executors available, so it is best to start next wave
			assert(plan_->isFinishingPossible(networking_failures_));
			++failed_reads;
			continue;
		}

		// Process poll's output -- read from chunkservers
		for (pollfd &poll_fd : poll_fds) {
			if (poll_fd.revents == 0) {
				continue;
			}

			ReadOperationExecutor &executor = executors_.at(poll_fd.fd);

			if (!readSomeData(params, poll_fd, executor)) {
				++failed_reads;
			}
		}

		// Check if we are finished now
		if (plan_->isReadingFinished(available_parts_)) {
			executions_finished_by_additional_operations_ += wave > 0;
			break;
		}
	}
}

/*! \brief Debug function for checking if plan is valid. */
void ReadPlanExecutor::checkPlan(uint8_t *buffer_start) {
	(void)buffer_start;
#ifndef NDEBUG
	for (const auto &type_and_op : plan_->read_operations) {
		assert(type_and_op.first.isValid());
		const ReadPlan::ReadOperation &op(type_and_op.second);
		assert(op.request_offset >= 0 && op.request_size >= 0);
		assert((op.request_offset + op.request_size) <= MFSCHUNKSIZE);
		assert(op.buffer_offset >= 0 &&
		       (op.buffer_offset + op.request_size) <= plan_->read_buffer_size);

		if (op.request_size <= 0) {
			continue;
		}

		for (const auto &type_and_op2 : plan_->read_operations) {
			if (&type_and_op == &type_and_op2) {
				continue;
			}
			const ReadPlan::ReadOperation &op2(type_and_op2.second);
			bool overlap = true;

			assert(type_and_op.first != type_and_op2.first);

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
	for (const auto &post : plan_->postprocess_operations) {
		assert(post.first >= 0);
		post_size += post.first;
	}

	plan_->buffer_start = buffer_start;
	plan_->buffer_read = buffer_start + plan_->readOffset();
	plan_->buffer_end = buffer_start + plan_->fullBufferSize();

	assert(plan_->buffer_read >= plan_->buffer_start && plan_->buffer_read < plan_->buffer_end);
	assert(plan_->buffer_start < plan_->buffer_end);
#endif
}

void ReadPlanExecutor::executePlan(std::vector<uint8_t> &buffer,
		const ChunkTypeLocations &locations, ChunkConnector &connector,
		int connect_timeout, int level_timeout,
		const Timeout &total_timeout) {
	executors_.clear();
	networking_failures_.clear();
	available_parts_.clear();
	++executions_total_;

	std::size_t initial_size_of_buffer = buffer.size();
	buffer.resize(initial_size_of_buffer + plan_->fullBufferSize());

	checkPlan(buffer.data() + initial_size_of_buffer);

	ExecuteParams params{buffer.data() + initial_size_of_buffer + plan_->readOffset(), locations,
	                     connector, connect_timeout, level_timeout, total_timeout};

	try {
		executeReadOperations(params);
		int result_size =
		    plan_->postProcessData(buffer.data() + initial_size_of_buffer, available_parts_);
		buffer.resize(initial_size_of_buffer + result_size);
	} catch (Exception &) {
		for (const auto &fd_and_executor : executors_) {
			tcpclose(fd_and_executor.first);
			stats_.unregisterReadOperation(fd_and_executor.second.server());
		}
		buffer.resize(initial_size_of_buffer);
		throw;
	}

	for (const auto &fd_and_executor : executors_) {
		tcpclose(fd_and_executor.first);
		stats_.unregisterReadOperation(fd_and_executor.second.server());
	}
}
