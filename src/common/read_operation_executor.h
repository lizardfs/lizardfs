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

#pragma once

#include "common/platform.h"

#include <unistd.h>

#include "common/network_address.h"
#include "protocol/packet.h"
#include "common/read_planner.h"
#include "common/time_utils.h"

class ReadOperationExecutor {
public:
	ReadOperationExecutor(
			const ReadPlan::ReadOperation& readOperation,
			uint64_t chunkId,
			uint32_t chunkVersion,
			const ChunkPartType& chunkType,
			const NetworkAddress& server,
			int fd,
			uint8_t* buffer);

	ReadOperationExecutor(const ReadOperationExecutor&) = delete;
	ReadOperationExecutor(ReadOperationExecutor&&) = default;

	/*
	 * Prepares (LIZ_)CLTOCS_READ message and sends it to the chunkserver
	 */
	void sendReadRequest(const Timeout& timeout);

	/*
	 * Executes read operation on the socket and processes the received data.
	 * This function calls read only once, so it can be used together with poll
	 * to execute many operations in parallel.
	 * It should be called many times until isFinished() is false.
	 */
	void continueReading();

	/*
	 * Executes continueReading() until the operation is finished
	 */
	void readAll(const Timeout& timeout);

	/**
	 * Checks if the read operation is finished.
	 */
	bool isFinished() const {
		return state_ == kFinished;
	}

	/**
	 * A getter.
	 */
	ChunkPartType chunkType() const {
		return chunkType_;
	}

	/**
	 * A getter.
	 */
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
		kFinished
	};

	/* The buffer which will be used to receive messages from a chunkserver */
	std::vector<uint8_t> messageBuffer_;

	/* Most recently received message header */
	PacketHeader packetHeader_;

	/* Read operation that this object will execute */
	ReadPlan::ReadOperation readOperation_;

	/* The buffer, where the data will be placed according to readOperation_.offsetsOfBlocks */
	uint8_t* const dataBuffer_;

	/* Information about the chunk, that will be used to execute the red operation */
	const uint64_t chunkId_;
	const uint32_t chunkVersion_;
	const ChunkPartType chunkType_;

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

	/* checksum will be used to receive crc of complete data blocks */
	uint32_t currentlyReadBlockCrc_;

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
