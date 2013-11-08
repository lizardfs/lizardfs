#ifndef LIZARDFS_MFSMOUNT_CHUNK_CONNECTOR_H_
#define LIZARDFS_MFSMOUNT_CHUNK_CONNECTOR_H_

#include "common/connection_pool.h"

class ChunkConnector {
public:
	ChunkConnector(uint32_t sourceIp, ConnectionPool& connectionPool);
	int connect(const NetworkAddress& server) const;

	void returnToPool(int fd, const NetworkAddress& server, int timeout) const {
		connectionPool_.putConnection(fd, server, timeout);
	}

private:
	static const uint8_t kMaxConnectionRetries = 5;
	uint32_t sourceIp_;
	ConnectionPool& connectionPool_;
};

#endif // LIZARDFS_MFSMOUNT_CHUNK_CONNECTOR_H_
