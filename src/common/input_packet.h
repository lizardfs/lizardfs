/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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

#include "common/exception.h"
#include "common/packet.h"
#include "common/massert.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(InputPacketTooLongException, Exception);

/// A struct used in servers for reading data from clients.
struct InputPacket {
	InputPacket(uint32_t maxPacketSize) : maxPacketSize_(maxPacketSize), bytesRead_(0U) {}

	/// Prepares object to start reading a new packet.
	void reset() {
		bytesRead_ = 0;
		data_.clear();
	}

	/// How many bytes should be passed to \p ::read.
	uint32_t bytesToBeRead() const {
		if (hasHeader()) {
			uint32_t dataBytesRead = bytesRead_ - PacketHeader::kSize;
			return data_.size() - dataBytesRead;
		} else {
			return PacketHeader::kSize - bytesRead_;
		}
	}

	/// Pointer which should be passed to \p ::read.
	uint8_t* pointerToBeReadInto() {
		if (hasHeader()) {
			size_t offset = bytesRead_ - PacketHeader::kSize;
			sassert(offset <= data_.size());
			return data_.data() + offset;
		} else {
			return &(header_[bytesRead_]);
		}
	}

	/// Should be called after each successful \p ::read.
	/// \throws InputPacketTooLongException
	void increaseBytesRead(uint32_t additionalBytesRead) {
		bytesRead_ += additionalBytesRead;
		if (bytesRead_ == PacketHeader::kSize) {
			auto messageDataLength = getHeader().length;
			if (messageDataLength > maxPacketSize_) {
				throw InputPacketTooLongException(
						"packet too long (" + std::to_string(messageDataLength) +
						"/" + std::to_string(maxPacketSize_) + ")");
			}
			data_.resize(messageDataLength);
		}
	}

	/// Is header already received?
	bool hasHeader() const {
		return (bytesRead_ >= PacketHeader::kSize);
	}

	/// Returns header of a message inside this \p InputPacket.
	/// Valid iff hasHeader() == true.
	PacketHeader getHeader() const {
		try {
			PacketHeader packetHeader;
			deserializePacketHeader(header_, sizeof(header_), packetHeader);
			return packetHeader;
		} catch (IncorrectDeserializationException&) {
			// Should NEVER happen!
			mabort("InputPacket::getHeader() did throw IncorrectDeserializationException");
		}
	}

	/// Is the whole message already received?
	bool hasData() const {
		return (bytesToBeRead() == 0);
	}

	/// Returns data of a message inside this \p InputPacket.
	/// Valid iff hasData() == true.
	const MessageBuffer& getData() const {
		return data_;
	}

private:
	/// Maximum accepted length of a message.
	const uint32_t maxPacketSize_;

	/// PacketHeader read from network in its serialized form
	uint8_t header_[PacketHeader::kSize];

	/// Message data read from network in its serialized form
	MessageBuffer data_;

	/// Amount of bytes stored in \p header and \p data
	uint32_t bytesRead_;
};
