#include "common/platform.h"
#include "common/read_plan_executor.h"

#include <sys/poll.h>
#include <chrono>
#include <map>
#include <set>

#include "common/block_xor.h"
#include "common/chunkserver_stats.h"
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

	// This closes all the opened TCP connections when this function returns or throws
	auto disconnector = makeLambdaGuard([&]() {
		for (const auto& fdAndExecutor : executors) {
			tcpclose(fdAndExecutor.first);
		}
	});

	// A function which starts a new operation. Returns a file descriptor of the created socket.
	auto startReadOperation = [&](ChunkType chunkType, const ReadPlan::ReadOperation& op) {
		if (networkingFailures.count(chunkType) != 0) {
			// Don't even try to start any additional operations from a chunkserver that
			// already failed before, because we won't be able to use the downloaded data.
			return -1;
		}
		sassert(locations.count(chunkType) == 1);
		const NetworkAddress& server = locations.at(chunkType);
		statsProxy.registerReadOperation(server);
		try {
			Timeout connectTimeout(std::chrono::milliseconds(timeouts.connectTimeout_ms));
			int fd = connector.startUsingConnection(server, connectTimeout);
			if (totalTimeout.expired()) {
				// totalTimeout might expire during establishing the connection
				throw RecoverableReadException("Chunkserver communication timed out");
			}
			ReadOperationExecutor executor(op,
					chunkId_, chunkVersion_, chunkType,
					server, fd, buffer);
			executor.sendReadRequest(connectTimeout);
			executors.insert(std::make_pair(fd, std::move(executor)));
			descriptorsForBasicReadOperations.insert(fd);
			return fd;
		} catch (ChunkserverConnectionException& ex) {
			lastConnectionFailure = server;
			statsProxy.markDefective(server);
			networkingFailures.insert(chunkType);
			return -1;
		}
	};

	// A function which verifies if we are able to finish executing
	// the plan if there were any networking failures
	auto isFinishingPossible = [&]() {
		if (networkingFailures.empty()) {
			return true;
		}
		bool anyBasicOperationFailed = descriptorsForBasicReadOperations.count(-1);
		return !anyBasicOperationFailed || plan_->isReadingFinished(networkingFailures);
	};

	// Connect to all needed chunkservers from basicReadOperations
	for (const auto& chunkTypeReadInfo : plan_->basicReadOperations) {
		int fd = startReadOperation(chunkTypeReadInfo.first, chunkTypeReadInfo.second);
		// fd may be equal to -1 in case of a failure, but we will insert it to the set anyway
		// to remember that some basic operation is unfinished all the time
		descriptorsForBasicReadOperations.insert(fd);
	}
	if (!isFinishingPossible()) {
		throw RecoverableReadException("Can't connect to " + lastConnectionFailure.toString());
	}

	// Receive responses
	LOG_AVG_TILL_END_OF_SCOPE0("ReadPlanExecutor::executeReadOperations#recv");
	Timeout basicTimeout(std::chrono::milliseconds(timeouts.basicTimeout_ms));
	bool additionalOperationsStarted = false;
	while (true) {
		if (!additionalOperationsStarted && basicTimeout.expired() && !totalTimeout.expired()) {
			// We have to start additionalReadOperations now
			for (const auto& chunkTypeReadInfo : plan_->additionalReadOperations) {
				startReadOperation(chunkTypeReadInfo.first, chunkTypeReadInfo.second);
			}
			if (!isFinishingPossible()) {
				throw RecoverableReadException("Can't connect to " +
						lastConnectionFailure.toString());
			}
			additionalOperationsStarted = true;
		}

		std::vector<pollfd> pollFds;
		for (const auto& fdAndExecutor : executors) {
			pollFds.push_back(pollfd());
			pollFds.back().fd = fdAndExecutor.first;
			pollFds.back().events = POLLIN;
			pollFds.back().revents = 0;
		}

		int pollTimeout = (basicTimeout.expired()
				? totalTimeout.remaining_ms()
				: basicTimeout.remaining_ms());
		int status = poll(pollFds.data(), pollFds.size(), pollTimeout);
		if (status < 0) {
			if (errno == EINTR) {
				continue;
			} else {
				throw RecoverableReadException("Poll error: " + std::string(strerr(errno)));
			}
		} else if (status == 0 && additionalOperationsStarted) {
			// The time is out, our chunkservers appear to be completely unresponsive.
			statsProxy.allPendingDefective();
			NetworkAddress offender = executors.begin()->second.server();
			throw RecoverableReadException(
					"Chunkserver communication timed out: " + offender.toString());
		}

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
				if (!isFinishingPossible()) {
					throw;
				}
			}
			if (executor.isFinished()) {
				statsProxy.unregisterReadOperation(server);
				statsProxy.markWorking(server);
				connector.endUsingConnection(fd, server);
				executors.erase(fd);
				descriptorsForBasicReadOperations.erase(fd);
				if (descriptorsForBasicReadOperations.empty()) {
					// all the basic operations are now finished.
					return plan_->getPostProcessOperationsForBasicPlan();
				}
			} else {
				unfinishedOperations.insert(executor.chunkType());
			}
			if (additionalOperationsStarted && plan_->isReadingFinished(unfinishedOperations)) {
				return plan_->getPostProcessOperationsForExtendedPlan(unfinishedOperations);
			}
		}
	}
	mabort("Bad code path -- reached an unreachable code");
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
