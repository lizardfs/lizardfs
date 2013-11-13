#ifndef LIZARDFS_MFSMOUNT_CHUNK_CONNECTOR_H_
#define LIZARDFS_MFSMOUNT_CHUNK_CONNECTOR_H_

#include "common/connection_pool.h"
#include "common/time_utils.h"

class ChunkConnector {
public:
	ChunkConnector(uint32_t sourceIp, ConnectionPool& connectionPool);
	int connect(const NetworkAddress& server, const Timeout& timeout) const;

	void returnToPool(int fd, const NetworkAddress& server, int timeout) const {
		connectionPool_.putConnection(fd, server, timeout);
	}

private:
	uint32_t sourceIp_;
	ConnectionPool& connectionPool_;
};

#endif // LIZARDFS_MFSMOUNT_CHUNK_CONNECTOR_H_
