#include "mount/chunk_connector.h"

#include <errno.h>
#include <algorithm>

#include "common/sockets.h"
#include "common/strerr.h"
#include "mount/exceptions.h"

static uint32_t timeoutTime(uint8_t tryCounter) {
	// JKZ's algorithm
	return (tryCounter % 2) ? (300 * (1 << (tryCounter >> 1))) : (200 * (1 << (tryCounter >> 1)));
}

ChunkConnector::ChunkConnector(uint32_t sourceIp, ConnectionPool& connectionPool)
		: sourceIp_(sourceIp),
		  connectionPool_(connectionPool){
}

int ChunkConnector::connect(const NetworkAddress& address) const {
	int fd = connectionPool_.getConnection(address);
	if (fd != -1) {
		return fd;
	}

	for (uint8_t i = 0; i < kMaxConnectionRetries; ++i) {
		fd = tcpsocket();
		if (fd < 0) {
			syslog(LOG_WARNING, "can't create tcp socket: %s", strerr(errno));
			break;
		}
		if (sourceIp_) {
			if (tcpnumbind(fd, sourceIp_, 0) < 0) {
				syslog(LOG_WARNING, "can't bind to given ip: %s", strerr(errno));
				tcpclose(fd);
				fd = -1;
				break;
			}
		}
		if (tcpnumtoconnect(fd, address.ip, address.port, timeoutTime(i)) < 0) {
			if (i >= kMaxConnectionRetries) {
				syslog(LOG_WARNING, "can't connect to (%08" PRIX32 ":%" PRIu16 "): %s",
						address.ip,
						address.port,
						strerr(errno));
			}
			tcpclose(fd);
			fd = -1;
		} else {
			break;
		}
	}
	if (fd < 0) {
		throw ChunkserverConnectionError(
				"Connection error: " + std::string(strerr(errno)), address);
	}
	if (tcpnodelay(fd) < 0) {
		syslog(LOG_WARNING,"can't set TCP_NODELAY: %s",strerr(errno));
	}
	return fd;
}
