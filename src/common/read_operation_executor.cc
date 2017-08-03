/*
   Copyright 2013-2017 Skytechnology sp. z o.o.

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
#include "common/read_operation_executor.h"

#include "common/crc.h"
#include "common/exceptions.h"
#include "common/lizardfs_version.h"
#include "common/massert.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "devtools/request_log.h"
#include "protocol/cltocs.h"
#include "protocol/cstocl.h"
#include "protocol/MFSCommunication.h"

static const uint32_t kMaxMessageLength = MFSBLOCKSIZE + 1024;

ReadOperationExecutor::ReadOperationExecutor(
		const ReadPlan::ReadOperation& readOperation,
		uint64_t chunkId,
		uint32_t chunkVersion,
		const ChunkPartType& chunkType,
		const NetworkAddress& server,
		uint32_t server_version,
		int fd,
		uint8_t* buffer)
		: readOperation_(readOperation),
		  dataBuffer_(buffer),
		  chunkId_(chunkId),
		  chunkVersion_(chunkVersion),
		  chunkType_(chunkType),
		  server_(server),
		  server_version_(server_version),
		  fd_(fd),
		  state_(kSendingRequest),
		  destination_(nullptr),
		  bytesLeft_(0),
		  dataBlocksCompleted_(0),
		  currentlyReadBlockCrc_(0) {
	messageBuffer_.reserve(cstocl::readData::kPrefixSize);
}

void ReadOperationExecutor::sendReadRequest(const Timeout& timeout) {
	std::vector<uint8_t> message;
	if (server_version_ >= kFirstECVersion) {
		cltocs::read::serialize(message, chunkId_, chunkVersion_, chunkType_,
			readOperation_.request_offset, readOperation_.request_size);
	} else if (server_version_ >= kFirstXorVersion) {
		cltocs::read::serialize(message, chunkId_, chunkVersion_, (legacy::ChunkPartType)chunkType_,
			readOperation_.request_offset, readOperation_.request_size);
	} else {
		serializeMooseFsPacket(message, CLTOCS_READ,
			chunkId_, chunkVersion_, readOperation_.request_offset,
			readOperation_.request_size);
	}

	int32_t ret = tcptowrite(fd_,
			message.data(),
			message.size(),
			timeout.remaining_ms());
	if (ret != (int32_t)message.size()) {
		throw ChunkserverConnectionException(
				"Cannot send READ request to the chunkserver: "
				+ std::string(strerr(tcpgetlasterror())),
				server_);
	}
	setState(kReceivingHeader);
}

void ReadOperationExecutor::continueReading() {
	sassert(state_ == kReceivingHeader
		|| state_ == kReceivingReadStatusMessage
		|| state_ == kReceivingReadDataMessage
		|| state_ == kReceivingDataBlock);

	ssize_t readBytes = tcprecv(fd_, destination_, bytesLeft_);
	if (readBytes == 0) {
		throw ChunkserverConnectionException(
				"Read from chunkserver error: connection reset by peer", server_);
	} else if (readBytes < 0 && tcpgetlasterror() == TCPEAGAIN) {
		return;
	} else if (readBytes < 0) {
		throw ChunkserverConnectionException(
				"Read from chunkserver error: " + std::string(strerr(tcpgetlasterror())), server_);
	}
	destination_ += readBytes;
	bytesLeft_ -= readBytes;
	if (bytesLeft_ > 0) {
		return;
	}
	switch (state_) {
		case kReceivingHeader:
			processHeaderReceived();
			break;
		case kReceivingReadStatusMessage:
			processReadStatusMessageReceived();
			break;
		case kReceivingReadDataMessage:
			processReadDataMessageReceived();
			break;
		case kReceivingDataBlock:
			processDataBlockReceived();
			break;
		default:
			massert(false, "Unknown state in ReadOperationExecutor::continueReading");
			break;
	}
}

void ReadOperationExecutor::readAll(const Timeout& timeout) {
	LOG_AVG_TILL_END_OF_SCOPE0("ReadOperationExecutor::readAll");
	struct pollfd pfd;
	pfd.fd = fd_;
	pfd.events = POLLIN;

	while (!isFinished()) {
		if (timeout.expired()) {
			throw ChunkserverConnectionException("Read from chunkserver: timeout", server_);
		}
		pfd.revents = 0;
		int ret = tcppoll(pfd, 50);

		if (ret < 0) {
#ifndef _WIN32
			if (errno == EINTR) {
				continue;
			}
#endif
			throw ChunkserverConnectionException(
					"Poll error: " + std::string(strerr(tcpgetlasterror())),
					server_);
		}
		if (pfd.revents & POLLIN) {
			continueReading();
		}
		if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
			throw ChunkserverConnectionException("Read (poll) from chunkserver error", server_);
		}
	}
}

void ReadOperationExecutor::processHeaderReceived() {
	sassert(state_ == kReceivingHeader);
	sassert(bytesLeft_ == 0);
	deserializePacketHeader(messageBuffer_, packetHeader_);
	if (packetHeader_.length > kMaxMessageLength) {
		std::stringstream ss;
		ss << "Message 0x" << std::hex << packetHeader_.type;
		ss << " sent by chunkserver too long (" << packetHeader_.length << " bytes)";
		throw ChunkserverConnectionException(ss.str(), server_);
	}
	if (packetHeader_.type == LIZ_CSTOCL_READ_DATA || packetHeader_.type == CSTOCL_READ_DATA) {
		setState(kReceivingReadDataMessage);
	} else if (packetHeader_.type == LIZ_CSTOCL_READ_STATUS || packetHeader_.type == CSTOCL_READ_STATUS) {
		setState(kReceivingReadStatusMessage);
	} else {
		std::stringstream ss;
		ss << "Unknown message 0x" << std::hex << packetHeader_.type;
		ss << " sent by chunkserver";
		throw ChunkserverConnectionException(ss.str(), server_);
	}
}

void ReadOperationExecutor::processReadDataMessageReceived() {
	sassert(state_ == kReceivingReadDataMessage);
	sassert(bytesLeft_ == 0);
	uint64_t readChunkId;
	uint32_t readOffset;
	uint32_t readSize;
	if (server_version_ >= kFirstXorVersion) {
		cstocl::readData::deserializePrefix(messageBuffer_, readChunkId, readOffset, readSize,
			currentlyReadBlockCrc_);
	} else {
		deserializeMooseFsPacketPrefixNoHeader(messageBuffer_.data(), messageBuffer_.size(),
			readChunkId, readOffset, readSize, currentlyReadBlockCrc_);
	}

	if (readChunkId != chunkId_) {
		std::stringstream ss;
		ss << "Malformed READ_DATA message from chunkserver, incorrect chunk ID ";
		ss << "(got: " << readChunkId << ", expected: " << chunkId_ << ")";
		throw ChunkserverConnectionException(ss.str(), server_);
	}
	if (readSize != MFSBLOCKSIZE) {
		std::stringstream ss;
		ss << "Malformed READ_DATA message from chunkserver, incorrect size ";
		ss << "(got: " << readSize << ", expected: " << MFSBLOCKSIZE << ")";
		throw ChunkserverConnectionException(ss.str(), server_);
	}
	uint32_t expectedOffset = readOperation_.request_offset + dataBlocksCompleted_ * MFSBLOCKSIZE;
	if (readOffset != expectedOffset) {
		std::stringstream ss;
		ss << "Malformed READ_DATA message from chunkserver, incorrect offset ";
		ss << "(got: " << readOffset << ", expected: " << expectedOffset << ")";
		throw ChunkserverConnectionException(ss.str(), server_);
	}
	setState(kReceivingDataBlock);
}

void ReadOperationExecutor::processReadStatusMessageReceived() {
	sassert(state_ == kReceivingReadStatusMessage);
	sassert(bytesLeft_ == 0);
	uint8_t readStatus;
	uint64_t readChunkId;

	if (server_version_ >= kFirstXorVersion) {
		cstocl::readStatus::deserialize(messageBuffer_, readChunkId, readStatus);
	} else {
		deserializeAllMooseFsPacketDataNoHeader(messageBuffer_.data(), messageBuffer_.size(),
			readChunkId, readStatus);
	}

	if (readChunkId != chunkId_) {
		std::stringstream ss;
		ss << "Malformed LIZ_CSTOCL_READ_STATUS message from chunkserver, ";
		ss << "incorrect chunk ID ";
		ss << "(got: " << readChunkId << ", expected: " << chunkId_ << ")";
		throw ChunkserverConnectionException(ss.str(), server_);
	}

	if (readStatus == LIZARDFS_ERROR_CRC) {
		throw ChunkCrcException("READ_DATA: corrupted data block (CRC mismatch)", server_, chunkType_);
	}

	if (readStatus != LIZARDFS_STATUS_OK) {
		std::stringstream ss;
		ss << "Status '" << lizardfs_error_string(readStatus) << "' sent by chunkserver";
		throw ChunkserverConnectionException(ss.str(), server_);
	}

	int totalDataBytesReceived = dataBlocksCompleted_ * MFSBLOCKSIZE;
	if (totalDataBytesReceived != readOperation_.request_size) {
		throw ChunkserverConnectionException(
				"READ_STATUS from chunkserver received too early", server_);
	}

	setState(kFinished);
}

void ReadOperationExecutor::processDataBlockReceived() {
	sassert(state_ == kReceivingDataBlock);
	sassert(bytesLeft_ == 0);

#ifdef ENABLE_CRC
	if (currentlyReadBlockCrc_ != mycrc32(0, destination_ - MFSBLOCKSIZE, MFSBLOCKSIZE)) {
		throw ChunkCrcException("READ_DATA: corrupted data block (CRC mismatch)", server_, chunkType_);
	}
#endif

	dataBlocksCompleted_++;
	setState(kReceivingHeader);
}

void ReadOperationExecutor::setState(ReadOperationState newState) {
	sassert(state_ != kFinished);
	sassert(bytesLeft_ == 0);
	switch (newState) {
	case kReceivingHeader:
		sassert(state_ == kSendingRequest || state_ == kReceivingDataBlock);
		messageBuffer_.resize(PacketHeader::kSize);
		destination_ = messageBuffer_.data();
		bytesLeft_ = messageBuffer_.size();
		break;
	case kReceivingReadStatusMessage:
		sassert(state_ == kReceivingHeader);
		messageBuffer_.resize(packetHeader_.length);
		destination_ = messageBuffer_.data();
		bytesLeft_ = messageBuffer_.size();
		break;
	case kReceivingReadDataMessage:
		sassert(state_ == kReceivingHeader);
		messageBuffer_.resize(server_version_ >= kFirstXorVersion
			? cstocl::readData::kPrefixSize : cstocl::readData::kLegacyPrefixSize);
		destination_ = messageBuffer_.data();
		bytesLeft_ = messageBuffer_.size();
		break;
	case kReceivingDataBlock:
		sassert(state_ == kReceivingReadDataMessage);
		destination_ =
		    dataBuffer_ + (readOperation_.buffer_offset + dataBlocksCompleted_ * MFSBLOCKSIZE);
		bytesLeft_ = MFSBLOCKSIZE;
		break;
	case kFinished:
		break;
	default:
		massert(false, "Unknown state in ReadOperationExecutor::setState");
		break;
	}
	state_ = newState;
}
