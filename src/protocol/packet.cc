/*
   Copyright 2013-2014 EditShare, 2013-2017 Skytechnology sp. z o.o.

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
#include "protocol/packet.h"

#include "common/sockets.h"

void receivePacket(PacketHeader& header, std::vector<uint8_t>& data, int sock,
		uint32_t timeout_ms) {
	sassert(data.empty());

	const int32_t headerSize = serializedSize(header);

	data.resize(headerSize);
	if (tcptoread(sock, data.data(), headerSize, timeout_ms) != headerSize) {
		tcpclose(sock);
		throw Exception("Did not manage to receive packet header");
	}

	deserializePacketHeader(data, header);

	const int32_t payloadSize = header.length;
	if (payloadSize > (int32_t)kMaxDeserializedBytesCount) {
		throw Exception("Too big packet data length");
	}
	data.resize(payloadSize);
	if (tcptoread(sock, data.data(), payloadSize, timeout_ms) != payloadSize) {
		tcpclose(sock);
		throw Exception("Did not manage to receive packet data");
	}
}
