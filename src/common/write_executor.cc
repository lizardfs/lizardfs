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
#include "common/write_executor.h"

#include <cstring>
#include <iostream>

#include "common/crc.h"
#include "common/exceptions.h"
#include "common/network_address.h"
#include "common/lizardfs_version.h"
#include "common/sockets.h"
#include "devtools/request_log.h"
#include "protocol/cltocs.h"
#include "protocol/cstocl.h"

const uint32_t kReceiveBufferSize = 1024;

WriteExecutor::WriteExecutor(ChunkserverStats& chunkserverStats,
		const NetworkAddress& headAddress, uint32_t chunkserver_version, int headFd,
		uint32_t responseTimeout_ms, uint64_t chunkId, uint32_t chunkVersion, ChunkPartType chunkType)
		: chunkserverStats_(chunkserverStats),
		  isRunning_(false),
		  chunkId_(chunkId),
		  chunkVersion_(chunkVersion),
		  chunkType_(chunkType),
		  chainHead_(headAddress),
		  chunkserver_version_(chunkserver_version),
		  chainHeadFd_(headFd),
		  receiveBuffer_(kReceiveBufferSize),
		  unconfirmedPackets_(0),
		  responseTimeout_(std::chrono::milliseconds(responseTimeout_ms)) {
	chunkserverStats_.registerWriteOperation(chainHead_);
}

WriteExecutor::~WriteExecutor() {
	chunkserverStats_.unregisterWriteOperation(chainHead_);
	for (const auto& server : chain_) {
		chunkserverStats_.unregisterWriteOperation(server.address);
	}
}

void WriteExecutor::addChunkserverToChain(const ChunkTypeWithAddress& type_with_address) {
	sassert(!isRunning_);
	chain_.push_back(type_with_address);
	chunkserverStats_.registerWriteOperation(type_with_address.address);
}

void WriteExecutor::addInitPacket() {
	sassert(!isRunning_);
	sassert(unconfirmedPackets_ == 0);
	pendingPackets_.push_back(Packet());
	std::vector<uint8_t>& buffer = pendingPackets_.back().buffer;
	std::stable_sort(chain_.begin(), chain_.end(),
		[](const ChunkTypeWithAddress& first, const ChunkTypeWithAddress &second) {
			return first.chunkserver_version > second.chunkserver_version;
	});
	if (!chain_.empty() && chain_.front().chunkserver_version < kFirstECVersion &&
	    (int)chunkType_.getSliceType() < Goal::Slice::Type::kECFirst) {
		std::vector<NetworkAddress> legacy_chain;
		legacy_chain.reserve(chain_.size());
		for (const auto &link : chain_) {
			legacy_chain.push_back(link.address);
		}
		cltocs::writeInit::serialize(buffer, chunkId_, chunkVersion_,
		                             static_cast<legacy::ChunkPartType>(chunkType_), legacy_chain);
	} else {
		cltocs::writeInit::serialize(buffer, chunkId_, chunkVersion_, chunkType_, chain_);
	}
	increaseUnconfirmedPacketCount();
	isRunning_ = true;
}

void WriteExecutor::addDataPacket(uint32_t writeId,
		uint16_t block, uint32_t offset, uint32_t size, const uint8_t* data) {
	sassert(isRunning_);
	uint32_t crc;
	{
		LOG_AVG_TILL_END_OF_SCOPE0("WriteExecutor::addDataPacket::mycrc32");
		crc = mycrc32(0, data, size);
	}
	pendingPackets_.push_back(Packet());
	Packet& packet = pendingPackets_.back();
	cltocs::writeData::serializePrefix(packet.buffer,
			chunkId_, writeId, block, offset, size, crc);
	packet.data = data;
	packet.dataSize = size;

	increaseUnconfirmedPacketCount();
}

void WriteExecutor::addEndPacket() {
	sassert(isRunning_);
	pendingPackets_.push_back(Packet());
	std::vector<uint8_t>& buffer = pendingPackets_.back().buffer;
	cltocs::writeEnd::serialize(buffer, chunkId_);
}

void WriteExecutor::sendData() {
	LOG_AVG_TILL_END_OF_SCOPE0("WriteExecutor::sendData");
	if (!bufferWriter_.hasDataToSend()) {
		if (pendingPackets_.empty()) {
			return;
		}
		const Packet& packet = pendingPackets_.front();
		bufferWriter_.addBufferToSend(packet.buffer.data(), packet.buffer.size());
		if (packet.data != nullptr) {
			bufferWriter_.addBufferToSend(packet.data, packet.dataSize);
		}
	}

	ssize_t bytesSent = bufferWriter_.writeTo(chainHeadFd_);
	if (bytesSent == 0) {
		throw ChunkserverConnectionException("Write error: connection closed by peer", server());
	} else if (bytesSent < 0 && tcpgetlasterror() != TCPEAGAIN) {
		throw ChunkserverConnectionException(
				"Write error: " + std::string(strerr(tcpgetlasterror())), server());
	}
	if (!bufferWriter_.hasDataToSend()) {
		bufferWriter_.reset();
		pendingPackets_.pop_front();
	}
}

std::vector<WriteExecutor::Status> WriteExecutor::receiveData() {
	ssize_t bytesRecv = receiveBuffer_.readFrom(chainHeadFd_);
	if (bytesRecv == 0) {
		throw ChunkserverConnectionException(
				"Read from chunkserver: connection closed by peer", server());
	} else if (bytesRecv < 0 && tcpgetlasterror() != TCPEAGAIN) {
		throw ChunkserverConnectionException(
				"Read from chunkserver: " + std::string(strerr(tcpgetlasterror())), server());
	}
	// Reset timer after each data read from socket
	responseTimeout_.reset();

	std::vector<WriteExecutor::Status> statuses;
	while(receiveBuffer_.hasMessageData()) {
		PacketHeader header = receiveBuffer_.getMessageHeader();
		// TODO deserialize without copying into vector
		std::vector<uint8_t> messageData(receiveBuffer_.getMessageData(),
				receiveBuffer_.getMessageData() + header.length);
		switch (header.type) {
			case LIZ_CSTOCL_WRITE_STATUS:
				statuses.push_back(processStatusMessage(messageData));
				if (unconfirmedPackets_ == 0) {
					throw RecoverableWriteException("Received too many statuses from chunkservers");
				}
				unconfirmedPackets_--;
				break;
			default:
				throw RecoverableWriteException("Received unknown message from chunkserver ("
						+ std::to_string(header.type) + ")");
		}
		receiveBuffer_.removeMessage();
	}
	return statuses;
}

bool WriteExecutor::serverTimedOut() const {
	// Response timeout makes sense only when there's any write in progress
	return unconfirmedPackets_ > 0 && responseTimeout_.expired();
}

void WriteExecutor::increaseUnconfirmedPacketCount() {
	unconfirmedPackets_++;
	// Start counting if we have just added a packet to executor that has been idle for a while
	if (unconfirmedPackets_ == 1) {
		responseTimeout_.reset();
	}
}

WriteExecutor::Status WriteExecutor::processStatusMessage(const std::vector<uint8_t>& message) {
	Status status;
	cstocl::writeStatus::deserialize(message, status.chunkId, status.writeId, status.status);
	return status;
}
