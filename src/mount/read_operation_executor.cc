#include "read_operation_executor.h"

#include "common/cltocs_communication.h"
#include "common/crc.h"
#include "common/cstocl_communication.h"
#include "common/massert.h"
#include "common/MFSCommunication.h"
#include "common/sockets.h"
#include "mount/exceptions.h"

static const uint32_t kSocketWriteTimeoutInMilliseconds = 2000;
static const uint32_t kMaxMessageLength = 65 * 1024;

ReadOperationExecutor::ReadOperationExecutor(
		const ReadOperationPlanner::ReadOperation& readOperation,
		uint64_t chunkId,
		uint32_t chunkVersion,
		const ChunkType& chunkType,
		const NetworkAddress& server,
		int fd,
		uint8_t* buffer)
		: readOperation_(readOperation),
		  dataBuffer_(buffer),
		  chunkId_(chunkId),
		  chunkVersion_(chunkVersion),
		  chunkType_(chunkType),
		  server_(server),
		  fd_(fd),
		  state_(kSendingRequest),
		  destination_(nullptr),
		  bytesLeft_(0),
		  dataBlocksCompleted_(0),
		  currentlyReadBlockCrc_(0) {
	messageBuffer_.reserve(cstocl::readData::kPrefixSize);
}

void ReadOperationExecutor::sendReadRequest() {
	std::vector<uint8_t> message;
#ifdef USE_LEGACY_READ_MESSAGES
	serializeMooseFsPacket(message, CLTOCS_READ,
			chunkId_, chunkVersion_, readOperation_.offset, readOperation_.size);
#else
	cltocs::read::serialize(message, chunkId_, chunkVersion_, chunkType_,
			readOperation_.requestOffset, readOperation_.requestSize);
#endif
	int32_t ret = tcptowrite(fd_,
			message.data(),
			message.size(),
			kSocketWriteTimeoutInMilliseconds);
	if (ret != (int32_t)message.size()) {
		throw ChunkserverConnectionError(
				"Cannot send READ request to the chunkserver: " + std::string(strerr(errno)),
				server_);
	}
	setState(kReceivingHeader);
}

void ReadOperationExecutor::continueReading() {
	sassert(state_ == kReceivingHeader
		|| state_ == kReceivingReadStatusMessage
		|| state_ == kReceivingReadDataMessage
		|| state_ == kReceivingDataBlock);

	ssize_t readBytes = read(fd_, destination_, bytesLeft_);
	if (readBytes == 0) {
		throw ChunkserverConnectionError(
				"Read from chunkserver error: connection reset by peer", server_);
	} else if (readBytes < 0) {
		throw ChunkserverConnectionError(
				"Read from chunkserver error: " + std::string(strerr(errno)), server_);
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

void ReadOperationExecutor::processHeaderReceived() {
	sassert(state_ == kReceivingHeader);
	sassert(bytesLeft_ == 0);
	deserializePacketHeader(messageBuffer_, packetHeader_);
	if (packetHeader_.length > kMaxMessageLength) {
		std::stringstream ss;
		ss << "Message 0x" << std::hex << packetHeader_.type;
		ss << " sent by chunkserver too long (" << packetHeader_.length << " bytes)";
		throw ChunkserverConnectionError(ss.str(), server_);
	}
	// TODO(msulikowski) Handle CSTOCL_READ_DATA and CSTOCL_READ_STATUS!
	if (packetHeader_.type == LIZ_CSTOCL_READ_DATA) {
		setState(kReceivingReadDataMessage);
	} else if (packetHeader_.type == LIZ_CSTOCL_READ_STATUS) {
		setState(kReceivingReadStatusMessage);
	} else {
		std::stringstream ss;
		ss << "Unknown message 0x" << std::hex << packetHeader_.type;
		ss << " sent by chunkserver";
		throw ChunkserverConnectionError(ss.str(), server_);
	}
}

void ReadOperationExecutor::processReadStatusMessageReceived() {
	sassert(state_ == kReceivingReadStatusMessage);
	sassert(bytesLeft_ == 0);
	uint8_t readStatus;
	uint64_t readChunkId;
	cstocl::readStatus::deserialize(messageBuffer_, readChunkId, readStatus);

	if (readChunkId != chunkId_) {
		std::stringstream ss;
		ss << "Malformed LIZ_CSTOCL_READ_STATUS message from chunkserver, ";
		ss << "incorrect chunk ID ";
		ss << "(got: " << readChunkId << ", expected: " << chunkId_ << ")";
		throw ChunkserverConnectionError(ss.str(), server_);
	}

	if (readStatus != STATUS_OK) {
		std::stringstream ss;
		ss << "Status '" << mfsstrerr(readStatus) << "'sent by chunkserver";
		throw ChunkserverConnectionError(ss.str(), server_);
	}

	uint32_t totalDataBytesReceived = dataBlocksCompleted_ * MFSBLOCKSIZE;
	if (totalDataBytesReceived != readOperation_.requestSize) {
		throw ChunkserverConnectionError(
				"READ_STATUS from chunkserver received too early", server_);
	}

	setState(kFinished);
}

void ReadOperationExecutor::processReadDataMessageReceived() {
	sassert(state_ == kReceivingReadDataMessage);
	sassert(bytesLeft_ == 0);
	uint64_t readChunkId;
	uint32_t readOffset;
	uint32_t readSize;
	cstocl::readData::deserializePrefix(messageBuffer_, readChunkId, readOffset, readSize,
			currentlyReadBlockCrc_);

	if (readChunkId != chunkId_) {
		std::stringstream ss;
		ss << "Malformed READ_DATA message from chunkserver, incorrect chunk ID ";
		ss << "(got: " << readChunkId << ", excpected: " << chunkId_ << ")";
		throw ChunkserverConnectionError(ss.str(), server_);
	}
	if (readSize != MFSBLOCKSIZE) {
		std::stringstream ss;
		ss << "Malformed READ_DATA message from chunkserver, incorrect size ";
		ss << "(got: " << readSize << ", excpected: " << MFSBLOCKSIZE << ")";
		throw ChunkserverConnectionError(ss.str(), server_);
	}
	uint32_t expectedOffset = readOperation_.requestOffset + dataBlocksCompleted_ * MFSBLOCKSIZE;
	if (readOffset != expectedOffset) {
		std::stringstream ss;
		ss << "Malformed READ_DATA message from chunkserver, incorrect offset ";
		ss << "(got: " << readOffset << ", excpected: " << expectedOffset << ")";
		throw ChunkserverConnectionError(ss.str(), server_);
	}
	setState(kReceivingDataBlock);
}

void ReadOperationExecutor::processDataBlockReceived() {
	sassert(state_ == kReceivingDataBlock);
	sassert(bytesLeft_ == 0);

#ifdef ENABLE_CRC
	if (currentlyReadBlockCrc_ != mycrc32(0, destination_ - MFSBLOCKSIZE, MFSBLOCKSIZE)) {
		throw ChunkserverConnectionError("READ_DATA: corrupted data block (CRC mismatch)", server_);
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
			messageBuffer_.resize(cstocl::readData::kPrefixSize);
			destination_ = messageBuffer_.data();
			bytesLeft_ = messageBuffer_.size();
			break;
		case kReceivingDataBlock:
			sassert(state_ == kReceivingReadDataMessage);
			sassert(readOperation_.destinationOffsets.size() > dataBlocksCompleted_);
			destination_ = dataBuffer_ + readOperation_.destinationOffsets[dataBlocksCompleted_];
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
