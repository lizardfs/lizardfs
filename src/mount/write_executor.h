#ifndef LIZARDFS_MOUNT_WRITE_EXECUTOR_H_
#define LIZARDFS_MOUNT_WRITE_EXECUTOR_H_

#include "common/chunk_type.h"
#include "common/message_receive_buffer.h"
#include "common/multi_buffer_writer.h"
#include "mount/chunk_reader.h"
#include "mount/chunkserver_stats.h"
#include "mount/chunkserver_write_chain.h"

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

	WriteExecutor(ChunkserverStats& chunkserverStats,
			const NetworkAddress& headAddress, int headFd,
			uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType);
	WriteExecutor(const WriteExecutor&) = delete;
	WriteExecutor& operator=(const WriteExecutor&) = delete;
	~WriteExecutor();

	void addChunkserverToChain(const NetworkAddress& address);
	void addInitPacket();
	void addDataPacket(uint32_t writeId,
			uint16_t block, uint32_t offset, uint32_t size, const uint8_t* data);
	void addEndPacket();
	void sendData();
	std::vector<Status> receiveData();

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

	Status processStatusMessage(const std::vector<uint8_t>& message);
};

#endif //LIZARDFS_MOUNT_WRITE_EXECUTOR_H_
