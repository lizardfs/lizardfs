#include "common/platform.h"
#include "common/read_plan_executor.h"

#include <sys/poll.h>
#include <chrono>
#include <map>

#include "common/block_xor.h"
#include "common/chunkserver_stats.h"
#include "common/exceptions.h"
#include "common/massert.h"
#include "common/mfserr.h"
#include "common/read_operation_executor.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "devtools/request_log.h"

ReadPlanExecutor::ReadPlanExecutor(
		ChunkserverStats& chunkserverStats,
		uint64_t chunkId, uint32_t chunkVersion,
		std::unique_ptr<ReadPlanner::Plan> plan)
		: chunkserverStats_(chunkserverStats),
		  chunkId_(chunkId),
		  chunkVersion_(chunkVersion),
		  plan_(std::move(plan)) {
}

void ReadPlanExecutor::executePlan(
		std::vector<uint8_t>& buffer,
		const ChunkTypeLocations& locations,
		ChunkConnector& connector,
		const Timeout& communicationTimeout) {
	const size_t initialSizeOfTheBuffer = buffer.size();
	buffer.resize(initialSizeOfTheBuffer + plan_->requiredBufferSize);
	try {
		uint8_t* dataBufferAddress = buffer.data() + initialSizeOfTheBuffer;
		executeReadOperations(dataBufferAddress, locations, connector, communicationTimeout);
		executeXorOperations(dataBufferAddress);
	} catch (Exception&) {
		buffer.resize(initialSizeOfTheBuffer);
		throw;
	}
}

void ReadPlanExecutor::executeReadOperations(
		uint8_t* buffer,
		const ChunkTypeLocations& locations,
		ChunkConnector& connector,
		const Timeout& communicationTimeout) {
	std::map<int, ReadOperationExecutor> executors;
	try {
		ChunkserverStatsProxy statsProxy(chunkserverStats_);
		// Connect to all needed chunkservers
		for (const auto& chunkTypeReadInfo : plan_->basicReadOperations) {
			const ChunkType chunkType = chunkTypeReadInfo.first;
			const ReadPlanner::ReadOperation& readOperation = chunkTypeReadInfo.second;
			sassert(locations.count(chunkType) == 1);
			const NetworkAddress& server = locations.at(chunkType);
			statsProxy.registerReadOperation(server);
			int fd = connector.startUsingConnection(server, communicationTimeout);
			ReadOperationExecutor executor(
					readOperation, chunkId_, chunkVersion_, chunkType, server, fd, buffer);
			executors.insert(std::make_pair(fd, executor));
		}

		// Send all the read requests
		for (auto& fdAndExecutor : executors) {
			fdAndExecutor.second.sendReadRequest(communicationTimeout);
		}

		LOG_AVG_TILL_END_OF_SCOPE0("ReadPlanExecutor::executeReadOperations#recv");
		// Receive responses
		while (!executors.empty()) {
			std::vector<pollfd> pollFds;
			for (const auto& fdAndExecutor : executors) {
				pollFds.push_back(pollfd());
				pollFds.back().fd = fdAndExecutor.first;
				pollFds.back().events = POLLIN;
				pollFds.back().revents = 0;
			}

			int status = poll(pollFds.data(), pollFds.size(), communicationTimeout.remaining_ms());
			if (status < 0) {
				if (errno == EINTR) {
					continue;
				} else {
					throw RecoverableReadException("Poll error: " + std::string(strerr(errno)));
				}
			} else if (status == 0) {
				// The time is out, our chunkservers appear to be unresponsive.
				statsProxy.allPendingDefective();
				// Report the first offender to callers.
				throw ChunkserverConnectionException(
					"Chunkserver communication timed out", executors.begin()->second.server());
			}

			for (pollfd& pollFd : pollFds) {
				int fd = pollFd.fd;
				ReadOperationExecutor& executor = executors.at(fd);
				const NetworkAddress& server = executor.server();
				if (!(pollFd.revents & POLLIN)) {
					if (pollFd.revents & (POLLHUP | POLLERR | POLLNVAL)) {
						throw ChunkserverConnectionException(
							"Read from chunkserver (poll) error", server);
					}
					continue;
				}
				executor.continueReading();
				if (executor.isFinished()) {
					statsProxy.unregisterReadOperation(server);
					statsProxy.markWorking(server);
					connector.endUsingConnection(fd, server);
					executors.erase(fd);
				}
			}
		}
	} catch (ChunkserverConnectionException &err) {
		chunkserverStats_.markDefective(err.server());
		for (const auto& fdAndExecutor : executors) {
			tcpclose(fdAndExecutor.first);
		}
		throw;
	} catch (Exception&) {
		for (const auto& fdAndExecutor : executors) {
			tcpclose(fdAndExecutor.first);
		}
		throw;
	}
}

void ReadPlanExecutor::executeXorOperations(uint8_t* buffer) {
	for (const auto& operation : plan_->getPostProcessOperationsForBasicPlan()) {
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
