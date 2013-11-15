#include "read_plan_executor.h"

#include <chrono>
#include <cstring>
#include <map>
#include <sys/poll.h>

#include "common/exceptions.h"
#include "common/massert.h"
#include "common/sockets.h"
#include "common/strerr.h"
#include "common/time_utils.h"
#include "mount/block_xor.h"
#include "mount/chunkserver_stats.h"
#include "mount/read_operation_executor.h"

static const uint32_t kConnectionPoolTimeoutInSeconds = 2;

ReadPlanExecutor::ReadPlanExecutor(uint64_t chunkId, uint32_t chunkVersion,
		const ReadOperationPlanner::Plan& plan)
		: chunkId_(chunkId),
		  chunkVersion_(chunkVersion),
		  plan_(plan) {
}

void ReadPlanExecutor::executePlan(
		std::vector<uint8_t>& buffer,
		const ChunkTypeLocations& locations,
		ChunkConnector& connector,
		const Timeout& communicationTimeout) {
	const size_t initialSizeOfTheBuffer = buffer.size();
	buffer.resize(initialSizeOfTheBuffer + plan_.requiredBufferSize);
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
		ChunkserverStatsProxy statsProxy(globalChunkserverStats);
		// Connect to all needed chunkservers
		for (const auto& chunkTypeReadInfo : plan_.readOperations) {
			const ChunkType chunkType = chunkTypeReadInfo.first;
			const ReadOperationPlanner::ReadOperation& readOperation = chunkTypeReadInfo.second;
			sassert(locations.count(chunkType) == 1);
			const NetworkAddress& server = locations.at(chunkType);
			statsProxy.registerReadOperation(server);
			int fd = connector.connect(server, communicationTimeout);
			ReadOperationExecutor executor(
					readOperation, chunkId_, chunkVersion_, chunkType, server, fd, buffer);
			executors.insert(std::make_pair(fd, executor));
		}

		// Send all the read requests
		for (auto& fdAndExecutor : executors) {
			fdAndExecutor.second.sendReadRequest(communicationTimeout);
		}

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
					throw RecoverableReadException("Poll error: " + std::string(strerror(errno)));
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
					connector.returnToPool(fd, server, kConnectionPoolTimeoutInSeconds);
					executors.erase(fd);
				}
			}
		}
	} catch (ChunkserverConnectionException &err) {
		globalChunkserverStats.markDefective(err.server());
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
	for (const ReadOperationPlanner::XorBlockOperation& xorOperation : plan_.xorOperations) {
		uint8_t* destination = buffer + xorOperation.destinationOffset;
		for (uint32_t sourceBlockOffset : xorOperation.blocksToXorOffsets) {
			const uint8_t* source = buffer + sourceBlockOffset;
			blockXor(destination, source, MFSBLOCKSIZE);
		}
	}
}
