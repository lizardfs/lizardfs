#include "common/platform.h"
#include "common/packet.h"

#include "common/sockets.h"

void receivePacket(PacketHeader& header, std::vector<uint8_t>& data, int sock,
		uint32_t timeout_ms) throw (Exception) {
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
