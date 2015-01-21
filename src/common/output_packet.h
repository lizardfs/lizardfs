#pragma once

#include "common/platform.h"

#include "common/packet.h"
#include "common/massert.h"

/// A struct used in servers for writing data to clients.
struct OutputPacket {
	OutputPacket(const PacketHeader& header) : bytesSent(0) {
		packet.reserve(PacketHeader::kSize + header.length);
		serialize(packet, header);
		packet.resize(PacketHeader::kSize + header.length);
	}

	OutputPacket(MessageBuffer message) : packet(std::move(message)), bytesSent(0) {}

	// Make it non-copyable (but movable) to avoid errors
	OutputPacket(OutputPacket&&) = default;
	OutputPacket(const OutputPacket&) = delete;
	OutputPacket& operator=(OutputPacket&&) = default;
	OutputPacket& operator=(const OutputPacket&) = delete;

	MessageBuffer packet;
	uint32_t bytesSent;
};
