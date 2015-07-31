/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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
#include "protocol/cltocs.h"
#include "common/exceptions.h"
#include "common/lambda_guard.h"
#include "common/massert.h"
#include "common/mfserr.h"
#include "common/read_operation_executor.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "devtools/request_log.h"

ReadPlanExecutor::ReadPlanExecutor(
		ChunkserverStats& chunkserverStats,
		uint64_t chunkId, uint32_t chunkVersion,
		std::unique_ptr<ReadPlan> plan)
		: chunkserverStats_(chunkserverStats),
		  chunkId_(chunkId),
		  chunkVersion_(chunkVersion),
		  plan_(std::move(plan)) {
}

void ReadPlanExecutor::executePlan(
		std::vector<uint8_t>& buffer,
		const ChunkTypeLocations& locations,
		ChunkConnector& connector,
		const Timeouts& timeouts,
		const Timeout& totalTimeout) {
	const size_t initialSizeOfTheBuffer = buffer.size();
	buffer.resize(initialSizeOfTheBuffer + plan_->requiredBufferSize);
	try {
		uint8_t* dataBufferAddress = buffer.data() + initialSizeOfTheBuffer;
		auto postProcessOperations = executeReadOperations(
				dataBufferAddress, locations, connector, timeouts, totalTimeout);
		executePostProcessing(postProcessOperations, dataBufferAddress);
	} catch (Exception&) {
		buffer.resize(initialSizeOfTheBuffer);
		throw;
	}
}

std::vector<ReadPlan::PostProcessOperation> ReadPlanExecutor::executeReadOperations(
		uint8_t* buffer,
		const ChunkTypeLocations& locations,
		ChunkConnector& connector,
		const Timeouts& timeouts,
		const Timeout& totalTimeout) {
	++executionsTotal;

	// Proxy used to update statistics in a RAII manner
	ChunkserverStatsProxy statsProxy(chunkserverStats_);

	// A map fd -> ReadOperationExecutor. Executors are used to read data from chunkservers.
	std::map<int, ReadOperationExecutor> executors;

	// Set of descriptors of sockets which are used by executors for basic read operations.
	// When this set becomes empty we know that all the basic read operations are finished.
	std::set<int> descriptorsForBasicReadOperations;

	// A set of chunk types from which we don't read data because of a networking problem
	std::set<ChunkType> networkingFailures;
	NetworkAddress lastConnectionFailure;  // last server to which we weren't able to connect

	const int kInvalidDescriptor = -1;

	// This closes all the opened TCP connections when this function returns or throws
	auto disconnector = makeLambdaGuard([&]() {
		for (const auto& fdAndExecutor : executors) {
			tcpclose(fdAndExecutor.first);
		}
	});

	// A function which starts a new operation. Returns a file descriptor of the created socket.
	auto startReadOperation = [&](ChunkType chunkType, const ReadPlan::ReadOperation& op) -> int {
		if (networkingFailures.count(chunkType) != 0) {
			// Don't even try to start any additional operations from a chunkserver that
			// already failed before, because we won't be able to use the downloaded data.
			return kInvalidDescriptor;
		}
		sassert(locations.count(chunkType) == 1);
		const NetworkAddress& server = locations.at(chunkType);
		statsProxy.registerReadOperation(server);
		try {
			Timeout connectTimeout(std::chrono::milliseconds(timeouts.connectTimeout_ms));
			int fd = connector.startUsingConnection(server, connectTimeout);
			try {
				if (totalTimeout.expired()) {
					// totalTimeout might expire during establishing the connection
					throw RecoverableReadException("Chunkserver communication timed out");
				}
				ReadOperationExecutor executor(op,
						chunkId_, chunkVersion_, chunkType,
						server, fd, buffer);
				executor.sendReadRequest(connectTimeout);
				executors.insert(std::make_pair(fd, std::move(executor)));
			} catch (...) {
				tcpclose(fd);
				throw;
			}
			return fd;
		} catch (ChunkserverConnectionException& ex) {
			lastConnectionFailure = server;
			statsProxy.markDefective(server);
			networkingFailures.insert(chunkType);
			return kInvalidDescriptor;
		}
	};

	// A function which starts a new prefetch operation. Does not return a status.
	auto startPrefetchOperation = [&](ChunkType chunkType, const ReadPlan::PrefetchOperation& op)
			-> void {
		sassert(locations.count(chunkType) == 1);
		const NetworkAddress& server = locations.at(chunkType);
		try {
			Timeout connectTimeout(std::chrono::milliseconds(timeouts.connectTimeout_ms));
			int fd = connector.startUsingConnection(server, connectTimeout);
			try {
				if (totalTimeout.expired()) {
					// totalTimeout might expire during establishing the connection
					throw RecoverableReadException("Chunkserver communication timed out");
				}
				std::vector<uint8_t> message;
				cltocs::prefetch::serialize(message, chunkId_, chunkVersion_, chunkType,
						op.requestOffset, op.requestSize);
				int32_t ret = tcptowrite(fd, message.data(), message.size(),
						connectTimeout.remaining_ms());
				if (ret != (int32_t)message.size()) {
					throw ChunkserverConnectionException(
							"Cannot send PREFETCH request to the chunkserver: "
									+ std::string(strerr(errno)),
							server);
				}
			} catch (...) {
				tcpclose(fd);
				throw;
			}
			connector.endUsingConnection(fd, server);
		} catch (ChunkserverConnectionException& ex) {
			// That's a pity
		}
	};

	// A function which verifies if we are able to finish executing
	// the plan if there were any networking failures
	auto isFinishingPossible = [&]() -> bool {
		if (networkingFailures.empty()) {
			return true;
		}
		bool anyBasicOperationFailed = descriptorsForBasicReadOperations.count(kInvalidDescriptor);
		return !anyBasicOperationFailed || plan_->isReadingFinished(networkingFailures);
	};

	// Connect to all needed chunkservers from basicReadOperations
	for (const auto& chunkTypeReadInfo : plan_->basicReadOperations) {
		int fd = startReadOperation(chunkTypeReadInfo.first, chunkTypeReadInfo.second);
		// fd may be equal to kInvalidDescriptor in case of a failure, but we will insert it to
		// the set anyway to remember that some basic operation is unfinished all the time
		descriptorsForBasicReadOperations.insert(fd);
	}
	if (!isFinishingPossible()) {
		throw RecoverableReadException("Can't connect to " + lastConnectionFailure.toString());
	}
	// Send prefetch request for data that is expected to be needed soon (but not now)
	for (const auto& prefetchOperation : plan_->prefetchOperations) {
		startPrefetchOperation(prefetchOperation.first, prefetchOperation.second);
	}

	// Receive responses
	LOG_AVG_TILL_END_OF_SCOPE0("ReadPlanExecutor::executeReadOperations#recv");
	Timeout basicTimeout(std::chrono::milliseconds(timeouts.basicTimeout_ms));
	bool additionalOperationsStarted = false;
	while (true) {
		if (!additionalOperationsStarted
				&& (basicTimeout.expired()
						|| descriptorsForBasicReadOperations.count(kInvalidDescriptor) != 0)
				&& !totalTimeout.expired()) {
			// We have to start additionalReadOperations now
			for (const auto& chunkTypeReadInfo : plan_->additionalReadOperations) {
				startReadOperation(chunkTypeReadInfo.first, chunkTypeReadInfo.second);
			}
			if (!isFinishingPossible()) {
				throw RecoverableReadException("Can't connect to " +
						lastConnectionFailure.toString());
			}
			additionalOperationsStarted = true;
			++executionsWithAdditionalOperations;
		}

		// Prepare for poll
		std::vector<pollfd> pollFds;
		for (const auto& fdAndExecutor : executors) {
			pollFds.push_back(pollfd());
			pollFds.back().fd = fdAndExecutor.first;
			pollFds.back().events = POLLIN;
			pollFds.back().revents = 0;
		}

		// Call poll
		int pollTimeout = (basicTimeout.expired()
				? totalTimeout.remaining_ms()
				: basicTimeout.remaining_ms());
		int status = tcppoll(pollFds, pollTimeout);
		if (status < 0) {
			if (errno == EINTR) {
				continue;
			} else {
				throw RecoverableReadException("Poll error: " + std::string(strerr(errno)));
			}
		} else if (status == 0 && totalTimeout.expired()) {
			// The time is out, our chunkservers appear to be completely unresponsive.
			statsProxy.allPendingDefective();
			NetworkAddress offender = executors.begin()->second.server();
			throw RecoverableReadException(
					"Chunkserver communication timed out: " + offender.toString());
		}

		// Process poll's output -- read from chunkservers
		std::set<ChunkType> unfinishedOperations = networkingFailures;
		for (pollfd& pollFd : pollFds) {
			int fd = pollFd.fd;
			ReadOperationExecutor& executor = executors.at(fd);
			const NetworkAddress& server = executor.server();
			try {
				if (pollFd.revents & POLLIN) {
					executor.continueReading();
				} else if (pollFd.revents & (POLLHUP | POLLERR | POLLNVAL)) {
					throw ChunkserverConnectionException(
							"Read from chunkserver (poll) error", server);
				}
			} catch (ChunkserverConnectionException& ex) {
				statsProxy.markDefective(server);
				networkingFailures.insert(executor.chunkType());
				if (descriptorsForBasicReadOperations.count(fd) != 0) {
					descriptorsForBasicReadOperations.erase(fd);
					descriptorsForBasicReadOperations.insert(kInvalidDescriptor);
				}
				tcpclose(fd);
				executors.erase(fd);
				if (!isFinishingPossible()) {
					throw;
				}
				continue;
			}
			if (executor.isFinished()) {
				statsProxy.unregisterReadOperation(server);
				statsProxy.markWorking(server);
				connector.endUsingConnection(fd, server);
				executors.erase(fd);
				descriptorsForBasicReadOperations.erase(fd);
				if (descriptorsForBasicReadOperations.empty()) {
					// All the basic operations are now finished. This condition will always
					// be false in kInvalidDescriptor is in descriptorsForBasicReadOperations.
					partsOmitted_ = networkingFailures;
					return plan_->getPostProcessOperationsForBasicPlan();
				}
			} else {
				unfinishedOperations.insert(executor.chunkType());
			}
		}

		// Check if we are finished now
		if (additionalOperationsStarted && plan_->isReadingFinished(unfinishedOperations)) {
			++executionsFinishedByAdditionalOperations;
			partsOmitted_ = networkingFailures;
			for (const auto& fdAndExecutor : executors) {
				partsOmitted_.insert(fdAndExecutor.second.chunkType());
			}
			return plan_->getPostProcessOperationsForExtendedPlan(unfinishedOperations);
		}
	}
	mabort("Bad code path; reached an unreachable code in ReadPlanExecutor::executeReadOperations");
}

void ReadPlanExecutor::executePostProcessing(
		const std::vector<ReadPlan::PostProcessOperation> operations,
		uint8_t* buffer) {
	for (const auto& operation : operations) {
		uint8_t* destination = buffer + operation.destinationOffset;
		if (operation.sourceOffset != operation.destinationOffset) {
			uint8_t* source = buffer + operation.sourceOffset;
			memcpy(destination, source, MFSBLOCKSIZE);
		}
		for (uint32_t sourceBlockOffset : operation.blocksToXorOffsets) {
			const uint8_t* source = buffer + sourceBlockOffset;
			blockXor(destination, source, MFSBLOCKSIZE);
		}
	}
}

std::atomic<uint64_t> ReadPlanExecutor::executionsTotal;
std::atomic<uint64_t> ReadPlanExecutor::executionsWithAdditionalOperations;
std::atomic<uint64_t> ReadPlanExecutor::executionsFinishedByAdditionalOperations;
