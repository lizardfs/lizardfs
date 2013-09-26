#ifndef LIZARDFS_MFSCOMMON_CLTOMA_COMMUNICATION_H_
#define LIZARDFS_MFSCOMMON_CLTOMA_COMMUNICATION_H_

#include "mfscommon/MFSCommunication.h"
#include "mfscommon/packet.h"

namespace cltoma {
namespace fuseReadChunk {

void serialize(std::vector<uint8_t>& destination, uint32_t inode, uint32_t dataBlockNumber) {
	serializePacket(destination, LIZ_CLTOMA_FUSE_READ_CHUNK, 0, inode, dataBlockNumber);
}
void deserialize(const std::vector<uint8_t>& source, uint32_t& inode, uint32_t& dataBlockNumber) {
	deserializePacketDataNoHeader(source, inode, dataBlockNumber);
}

} // namespace fuseReadChunk
} // namespace cltoma

#endif // LIZARDFS_MFSCOMMON_CLTOMA_COMMUNICATION_H_
