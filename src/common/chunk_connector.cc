/*
   Copyright 2013-2017 Skytechnology sp. z o.o.

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
#include "common/chunk_connector.h"

#include <errno.h>
#include <algorithm>

#include "common/exceptions.h"
#include "common/mfserr.h"
#include "common/sockets.h"
#include "common/time_utils.h"

static int64_t timeoutTime(int64_t rtt, uint8_t tryCounter) {
	return rtt * (1 << (tryCounter / 2)) * 3 / (tryCounter % 2 == 0 ? 3 : 2);
}

ChunkConnector::ChunkConnector(uint32_t sourceIp) : roundTripTime_ms_(20), sourceIp_(sourceIp) {
}

int ChunkConnector::startUsingConnection(const NetworkAddress& server,
		const Timeout& timeout) const {
	int fd = -1;
	int retries = 0;
	int err = ETIMEDOUT;  // we want to return ETIMEDOUT on timeout.expired()
	while (!timeout.expired()) {
		fd = tcpsocket();
		if (fd < 0) {
			err = tcpgetlasterror();
			lzfs_pretty_syslog(LOG_WARNING, "can't create tcp socket: %s", strerr(err));
			break;
		}
		if (sourceIp_) {
			if (tcpnumbind(fd, sourceIp_, 0) < 0) {
				err = tcpgetlasterror();
				lzfs_pretty_syslog(LOG_WARNING, "can't bind to given ip: %s", strerr(err));
				tcpclose(fd);
				fd = -1;
				break;
			}
		}
		int64_t connectTimeout = std::min(
				timeoutTime(roundTripTime_ms_, retries),
				timeout.remaining_ms());
		connectTimeout = std::max(int64_t(1), connectTimeout); // tcpnumtoconnect doesn't like 0
		if (tcpnumtoconnect(fd, server.ip, server.port, connectTimeout) < 0) {
			err = tcpgetlasterror();
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
		lzfs_pretty_syslog(LOG_WARNING, "can't set TCP_NODELAY: %s", strerr(tcpgetlasterror()));
	}
	return fd;
}

void ChunkConnector::endUsingConnection(int fd, const NetworkAddress& /* server */) const {
	tcpclose(fd);
}

ChunkConnectorUsingPool::ChunkConnectorUsingPool(ConnectionPool& connectionPool, uint32_t sourceIp)
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
