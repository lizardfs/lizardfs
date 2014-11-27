#include "common/platform.h"
#include "mount/chunkserver_write_chain.h"

#include "common/datapack.h"
#include "common/LFSCommunication.h"
#include "mount/csdb.h"
#include "mount/mastercomm.h"

ChunkserverWriteChain::ChunkserverWriteChain() {
}

ChunkserverWriteChain::~ChunkserverWriteChain() {
	for (const auto& server : servers_) {
		csdb_writedec(server.ip, server.port);
	}
}

void ChunkserverWriteChain::add(const NetworkAddress& server) {
	servers_.push_back(server);
	csdb_writeinc(server.ip, server.port);
}

int ChunkserverWriteChain::connect() {
	int fd = createNewChunkserverConnection(head());
	return fd;
}

void ChunkserverWriteChain::createInitialMessage(std::vector<uint8_t>& message,
		uint64_t chunkId, uint32_t version) {
	size_t messageDataSize = 12 + 6 * (servers_.size() - 1);
	message.resize(8 + messageDataSize); // 8 bytes of header + data

	uint8_t* data = message.data();

	// Message header
	put32bit(&data, CLTOCS_WRITE);
	put32bit(&data, messageDataSize);

	// Message data: chunk ID, version and servers other then the first one
	put64bit(&data, chunkId);
	put32bit(&data, version);
	for (size_t i = 1; i < servers_.size(); ++i) {
		put32bit(&data, servers_[i].ip);
		put16bit(&data, servers_[i].port);
	}
}

int ChunkserverWriteChain::createNewChunkserverConnection(const NetworkAddress& server) {
	uint32_t srcip = fs_getsrcip();
	unsigned tryCounter = 0;
	int fd = -1;
	while (fd == -1 && tryCounter < 10) {
		fd = tcpsocket();
		if (fd < 0) {
			syslog(LOG_WARNING, "can't create tcp socket: %s", strerr(errno));
			fd = -1;
			break;
		}
		if (srcip) {
			if (tcpnumbind(fd, srcip, 0) < 0) {
				syslog(LOG_WARNING, "can't bind socket to given ip: %s", strerr(errno));
				tcpclose(fd);
				fd = -1;
				break;
			}
		}
		int timeout;
		if (tryCounter % 2 == 0) {
			timeout = 200 * (1 << (tryCounter / 2));
		} else {
			timeout = 300 * (1 << (tryCounter / 2));
		}
		if (tcpnumtoconnect(fd, server.ip, server.port, timeout) < 0) {
			tryCounter++;
			if (tryCounter >= 10) {
				syslog(LOG_WARNING, "can't connect to (%08" PRIX32 ":%" PRIu16 "): %s",
						server.ip, server.port, strerr(errno));
			}
			tcpclose(fd);
			fd = -1;
		}
	}
	return fd;
}

