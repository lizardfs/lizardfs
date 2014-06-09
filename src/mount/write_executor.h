#pragma once

#include "config.h"

#include "common/chunk_type.h"
#include "common/chunkserver_stats.h"
#include "common/message_receive_buffer.h"
#include "common/multi_buffer_writer.h"
#include "mount/chunk_reader.h"

class WriteExecutor {
public:
	struct Status {
		uint64_t chunkId;
		uint32_t writeId;
		uint8_t status;

		Status() : chunkId(0), writeId(0), status(0) {}
		Status(uint64_t chunkId, uint32_t writeId, uint8_t status)
				: chunkId(chunkId),
				  writeId(writeId),
				  status(status) {
		}
	};

	/**
	 * Constructor
	 *
	 * \param chunkserverStats - database which will be updated by the object when accessing servers
	 * \param headAddress - an address of the chunkserver that is the head of the write chain
	 * \param headFd - a descriptor of the socket connected with \p headAddress
	 * \param responseTimeout_ms - a maximum time of waiting for a response from the chunkserver
	 * \param chunkId - a chunk that will be written to
	 * \param chunkVersion - a chunk that will be written to
	 * \param chunkType - a chunk that will be written to
	 */
	WriteExecutor(ChunkserverStats& chunkserverStats,
			const NetworkAddress& headAddress, int headFd, uint32_t responseTimeout_ms,
			uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType);
	WriteExecutor(const WriteExecutor&) = delete;
	~WriteExecutor();
	WriteExecutor& operator=(const WriteExecutor&) = delete;
	void addChunkserverToChain(const NetworkAddress& address);
	void addInitPacket();
	void addDataPacket(uint32_t writeId,
			uint16_t block, uint32_t offset, uint32_t size, const uint8_t* data);
	void addEndPacket();
	void sendData();
	std::vector<Status> receiveData();

	/**
	 * Checks if chunkserver (chain) has exceeded allowed response time
	 */
	bool serverTimedOut() const;

	uint32_t getPendingPacketCount() const {
		return pendingPackets_.size();
	}

	NetworkAddress server() const {
		return chainHead_;
	}

	ChunkType chunkType() const {
		return chunkType_;
	}

	int fd() const {
		return chainHeadFd_;
	}

private:
	struct Packet {
		std::vector<uint8_t> buffer;
		const uint8_t* data;
		uint32_t dataSize;

		Packet() : data(nullptr), dataSize(0) {}
	};

	ChunkserverStats& chunkserverStats_;
	bool isRunning_;
	const uint64_t chunkId_;
	const uint32_t chunkVersion_;
	const ChunkType chunkType_;
	std::vector<NetworkAddress> chain_;
	const NetworkAddress chainHead_;
	const int chainHeadFd_;
	std::list<Packet> pendingPackets_;
	MultiBufferWriter bufferWriter_;
	MessageReceiveBuffer receiveBuffer_;

	/// Number of WRITE_STATUS messages that are expected to be received from the chunkserver
	uint32_t unconfirmedPackets_;

	/// Measures time since last read from the chunkserver's socket
	Timeout responseTimeout_;

	/// Increases a counter and resets timer when needed
	void increaseUnconfirmedPacketCount();

	Status processStatusMessage(const std::vector<uint8_t>& message);
};
