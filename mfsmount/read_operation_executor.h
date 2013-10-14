#ifndef LIZARDFS_MFSMOUNT_READ_OPERATION_EXECUTOR_H_
#define LIZARDFS_MFSMOUNT_READ_OPERATION_EXECUTOR_H_

#include <unistd.h>

#include "mfscommon/network_address.h"
#include "mfscommon/packet.h"
#include "mfsmount/read_operation_planner.h"

class ReadOperationExecutor {
public:
	ReadOperationExecutor(
			const ReadOperationPlanner::ReadOperation& readOperation,
			uint64_t chunkId,
			uint32_t chunkVersion,
			const ChunkType& chunkType,
			const NetworkAddress& server,
			int fd,
			uint8_t* buffer);

	/*
	 * Prepares (LIZ_)CLTOCS_READ message and sends it to the chunkserver
	 */
	void sendReadRequest();

	/*
	 * Executes read operation on the socket and processes the received data.
	 * This function calls read only once, so it can be used together with poll
	 * to execute many operations in parallel.
	 * It should be called many times until isFinished() is false.
	 */
	void continueReading();

	bool isFinished() const {
		return state_ == kFinished;
	}

	const NetworkAddress& server() const {
		return server_;
	}

private:
	enum ReadOperationState {
		kSendingRequest,
		kReceivingHeader,
		kReceivingReadStatusMessage,
		kReceivingReadDataMessage,
		kReceivingDataBlock,
		kFinished,
	};

	/* The buffer which will be used to receive messages from a chunkserver */
	std::vector<uint8_t> messageBuffer_;

	/* Most recently received message header */
	PacketHeader packetHeader_;

	/* Read operation that this object will execute */
	const ReadOperationPlanner::ReadOperation readOperation_;

	/* The buffer, where the data will be placed according to readOperation_.offsetsOfBlocks */
	uint8_t* const dataBuffer_;

	/* Information about the chunk, that will be used to execute the red operation */
	const uint64_t chunkId_;
	const uint32_t chunkVersion_;
	const ChunkType chunkType_;

	/*Information about the server, that will be used to execute the read operation */
	const NetworkAddress server_;
	const int fd_;

	/* Current state of the operation */
	ReadOperationState state_;

	/* The address when the next data read from the socket should be placed */
	uint8_t* destination_;

	/* The amount of bytes which should be placed at destination_ in the current state */
	uint32_t bytesLeft_;

	/* Number of complete data blocks received from the chunkserver */
	uint32_t dataBlocksCompleted_;

	/*
	 * Four functions below are called when all the data
	 * in the corresponding state has been received
	 */
	void processHeaderReceived();
	void processReadStatusMessageReceived();
	void processReadDataMessageReceived();
	void processDataBlockReceived();

	/*
	 * After processing the data received in one state, this function is called
	 * to set the new state and prepare destination_ and bytesLeft_ variables
	 */
	void setState(ReadOperationState newState);
};

#endif // LIZARDFS_MFSMOUNT_READ_OPERATION_EXECUTOR_H_
