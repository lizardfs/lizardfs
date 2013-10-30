#include "read_plan_executor.h"

#include <map>
#include <sys/poll.h>

#include "common/massert.h"
#include "common/sockets.h"
#include "mount/block_xor.h"
#include "mount/exceptions.h"
#include "mount/read_operation_executor.h"

static const uint32_t kConnectionPoolTimeoutInSeconds = 2;
static const uint32_t kPollTimeoutInMilliseconds = 5000;

ReadPlanExecutor::ReadPlanExecutor(uint64_t chunkId, uint32_t chunkVersion,
		const ReadOperationPlanner::Plan& plan)
		: chunkId_(chunkId),
		  chunkVersion_(chunkVersion),
		  plan_(plan) {
}

void ReadPlanExecutor::executePlan(
		std::vector<uint8_t>& buffer,
		const ChunkTypeLocations& locations,
		ConnectionPool& connectionPool,
		ChunkConnector& connector) {
	const size_t initialSizeOfTheBuffer = buffer.size();
	buffer.resize(initialSizeOfTheBuffer + plan_.requiredBufferSize);
	try {
		uint8_t* dataBufferAddress = buffer.data() + initialSizeOfTheBuffer;
		executeReadOperations(dataBufferAddress, locations, connectionPool, connector);
		executeXorOperations(dataBufferAddress);
	} catch (Exception&) {
		buffer.resize(initialSizeOfTheBuffer);
		throw;
	}
}

void ReadPlanExecutor::executeReadOperations(
		uint8_t* buffer,
		const ChunkTypeLocations& locations,
		ConnectionPool& connectionPool,
		ChunkConnector& connector) {
	std::map<int, ReadOperationExecutor> executors;
	try {
		// Connect to all needed chunkservers
		for (const auto& chunkTypeReadInfo : plan_.readOperations) {
			const ChunkType chunkType = chunkTypeReadInfo.first;
			const ReadOperationPlanner::ReadOperation& readOperation = chunkTypeReadInfo.second;
			sassert(locations.count(chunkType) == 1);
			const NetworkAddress& server = locations.at(chunkType);
			int fd = connectionPool.getConnection(server);
			if (fd == -1) {
				fd = connector.connect(server);
			}
			ReadOperationExecutor executor(
					readOperation, chunkId_, chunkVersion_, chunkType, server, fd, buffer);
			executors.insert(std::make_pair(fd, executor));
		}

		// Send all the read requests
		for (auto& fdAndExecutor : executors) {
			fdAndExecutor.second.sendReadRequest();
		}

		while (!executors.empty()) {
			std::vector<pollfd> pollFds;
			for (const auto& fdAndExecutor : executors) {
				pollFds.push_back(pollfd());
				pollFds.back().fd = fdAndExecutor.first;
				pollFds.back().events = POLLIN;
				pollFds.back().revents = 0;
			}

			int status = poll(pollFds.data(), pollFds.size(), kPollTimeoutInMilliseconds);
			if (status < 0) {
				throw RecoverableReadError("Poll: " + std::string(strerr(errno)));
			} else if (status == 0) {
				throw RecoverableReadError("Poll timeout");
			}

			for (pollfd& pollFd : pollFds) {
				int fd = pollFd.fd;
				ReadOperationExecutor& executor = executors.at(fd);
				const NetworkAddress& server = executor.server();
				if (!(pollFd.revents & POLLIN)) {
					if (pollFd.revents & (POLLHUP | POLLERR | POLLNVAL)) {
						throw ChunkserverConnectionError(
							"Read from chunkserver (poll) error", server);
					}
					continue;
				}
				executor.continueReading();
				if (executor.isFinished()) {
					connectionPool.putConnection(fd, server, kConnectionPoolTimeoutInSeconds);
					executors.erase(fd);
				}
			}
		}
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
