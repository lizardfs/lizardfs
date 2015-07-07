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

#include <inttypes.h>
#include <vector>

#include "common/datapack.h"
#include "common/massert.h"
#include "common/packet.h"

/*
 * A class which can be used to read LizardFS commands from a socket
 */
class MessageReceiveBuffer {
public:
	/*
	 * @param size - maximum size of a message to read
	 */
	MessageReceiveBuffer(size_t size) : buffer_(size), bytesReceived_(0) {
	}

	/*
	 * Read some data from a descriptor into the buffer
	 */
	ssize_t readFrom(int fd);

	/*
	 * Removes the first message from the buffer
	 */
	void removeMessage();

	/*
	 * If this is true, one can access this->getMessageHeader()
	 */
	bool hasMessageHeader() const {
		return bytesReceived_ >= PacketHeader::kSize;
	}

	/*
	 * This is true when the whole message has been read.
	 */
	bool hasMessageData() const {
		if (!hasMessageHeader()) {
			return false;
		}
		return bytesReceived_ >= PacketHeader::kSize + getMessageHeader().length;
	}

	/*
	 * This is true if the message currently read is bigger than a size of the buffer,
	 * making it impossible to read the whole message body.
	 */
	bool isMessageTooBig() const {
		if (!hasMessageHeader()) {
			return false;
		}
		return PacketHeader::kSize + getMessageHeader().length > buffer_.size();
	}

	PacketHeader getMessageHeader() const {
		eassert(hasMessageHeader());
		PacketHeader header;
		deserialize(buffer_, header);
		return header;
	}

	const uint8_t* getMessageData() const {
		return buffer_.data() + PacketHeader::kSize;
	}

private:
	std::vector<uint8_t> buffer_;
	size_t bytesReceived_;
};
