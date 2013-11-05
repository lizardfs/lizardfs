#ifndef LIZARDFS_MFSMOUNT_CHUNK_CONNECTOR_H_
#define LIZARDFS_MFSMOUNT_CHUNK_CONNECTOR_H_

#include "common/network_address.h"

class ChunkConnector {
public:
	ChunkConnector(uint32_t sourceIp);
	int connect(const NetworkAddress& server) const;

private:
	static const uint8_t kMaxConnectionRetries = 5;
	uint32_t sourceIp_;
};

#endif // LIZARDFS_MFSMOUNT_CHUNK_CONNECTOR_H_
