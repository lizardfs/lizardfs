#include "common/platform.h"
#include "mount/chunkserver_write_chain.h"

#include "common/MFSCommunication.h"
#include "common/packet.h"
#include "common/serializable_range.h"
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
	sassert(!servers_.empty());
	serializeMooseFsPacket(message, CLTOCS_WRITE, chunkId, version,
			makeSerializableRange(servers_.begin() + 1, servers_.end()));
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

