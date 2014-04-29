#include "config.h"
#include "common/chunk_connector.h"

#include <errno.h>
#include <algorithm>

#include "common/sockets.h"
#include "common/strerr.h"
#include "common/time_utils.h"
#include "mount/exceptions.h"

static int64_t timeoutTime(uint8_t tryCounter) {
	// JKZ's algorithm
	return (tryCounter % 2) ? (30 * (1 << (tryCounter >> 1))) : (20 * (1 << (tryCounter >> 1)));
}

ChunkConnector::ChunkConnector(uint32_t sourceIp) : sourceIp_(sourceIp) {
}

int ChunkConnector::startUsingConnection(const NetworkAddress& server,
		const Timeout& timeout) const {
	int fd = -1;
	int retries = 0;
	int err = ETIMEDOUT;  // we want to return ETIMEDOUT on timeout.expired()
	while (!timeout.expired()) {
		fd = tcpsocket();
		if (fd < 0) {
			err = errno;
			syslog(LOG_WARNING, "can't create tcp socket: %s", strerr(errno));
			break;
		}
		if (sourceIp_) {
			if (tcpnumbind(fd, sourceIp_, 0) < 0) {
				err = errno;
				syslog(LOG_WARNING, "can't bind to given ip: %s", strerr(errno));
				tcpclose(fd);
				fd = -1;
				break;
			}
		}
		int64_t connectTimeout = std::min(timeoutTime(retries), timeout.remaining_ms());
		connectTimeout = std::max(int64_t(1), connectTimeout); // tcpnumtoconnect doesn't like 0
		if (tcpnumtoconnect(fd, server.ip, server.port, connectTimeout) < 0) {
			err = errno;
			tcpclose(fd);
			fd = -1;
		} else {
			break;
		}
		retries++;
	}
	if (fd < 0) {
		throw ChunkserverConnectionException(
				"Connection error: " + std::string(strerr(err)), server);
	}
	if (tcpnodelay(fd) < 0) {
		syslog(LOG_WARNING,"can't set TCP_NODELAY: %s",strerr(errno));
	}
	return fd;
}

void ChunkConnector::endUsingConnection(int fd, const NetworkAddress& /* server */) const {
	tcpclose(fd);
}

ChunkConnectorUsingPool::ChunkConnectorUsingPool(uint32_t sourceIp, ConnectionPool& connectionPool)
		: ChunkConnector(sourceIp),
		  connectionPool_(connectionPool) {
}

int ChunkConnectorUsingPool::startUsingConnection(const NetworkAddress& server,
		const Timeout& timeout) const {
	int fd = connectionPool_.getConnection(server);
	if (fd >= 0) {
		return fd;
	} else {
		return ChunkConnector::startUsingConnection(server, timeout);
	}
}

void ChunkConnectorUsingPool::endUsingConnection(int fd, const NetworkAddress& server) const {
	connectionPool_.putConnection(fd, server, kConnectionPoolTimeout_s);
}
