#pragma once

#include "common/connection_pool.h"
#include "common/time_utils.h"

class ChunkConnector {
public:
	static const uint32_t kConnectionPoolTimeout_s = 3;

	ChunkConnector(uint32_t sourceIp, ConnectionPool& connectionPool);
	int connect(const NetworkAddress& server, const Timeout& timeout) const;

	void returnToPool(int fd, const NetworkAddress& server) const {
		connectionPool_.putConnection(fd, server, kConnectionPoolTimeout_s);
	}

private:
	uint32_t sourceIp_;
	ConnectionPool& connectionPool_;
};
