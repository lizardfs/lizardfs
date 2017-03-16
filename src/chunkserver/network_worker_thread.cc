/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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
#include "chunkserver/network_worker_thread.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>
#include <atomic>
#include <cassert>
#include <memory>
#include <mutex>
#include <set>

#include "chunkserver/bgjobs.h"
#include "chunkserver/hdd_readahead.h"
#include "chunkserver/hddspacemgr.h"
#include "chunkserver/network_stats.h"
#include "common/cfg.h"
#include "common/charts.h"
#include "protocol/cltocs.h"
#include "protocol/cstocl.h"
#include "protocol/cstocs.h"
#include "common/datapack.h"
#include "common/event_loop.h"
#include "common/lizardfs_version.h"
#include "common/massert.h"
#include "protocol/MFSCommunication.h"
#include "common/moosefs_vector.h"
#include "protocol/packet.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "devtools/request_log.h"
#include "devtools/TracePrinter.h"

#define MaxPacketSize (100000 + MFSBLOCKSIZE)

// connection timeout in seconds
#define CSSERV_TIMEOUT 10

#define CONNECT_RETRIES 10
#define CONNECT_TIMEOUT(cnt) (((cnt)%2)?(300000*(1<<((cnt)>>1))):(200000*(1<<((cnt)>>1))))

class MessageSerializer {
public:
	static MessageSerializer* getSerializer(PacketHeader::Type type);

	virtual void serializePrefixOfCstoclReadData(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint32_t offset, uint32_t size) = 0;
	virtual void serializeCstoclReadStatus(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint8_t status) = 0;
	virtual void serializeCstoclWriteStatus(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint32_t writeId, uint8_t status) = 0;
	virtual ~MessageSerializer() {}
};

class MooseFsMessageSerializer : public MessageSerializer {
public:
	void serializePrefixOfCstoclReadData(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint32_t offset, uint32_t size) {
		// This prefix requires CRC (uint32_t) and data (size * uint8_t) to be appended
		uint32_t extraSpace = sizeof(uint32_t) + size;
		serializeMooseFsPacketPrefix(buffer, extraSpace, CSTOCL_READ_DATA, chunkId, offset, size);
	}

	void serializeCstoclReadStatus(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint8_t status) {
		serializeMooseFsPacket(buffer, CSTOCL_READ_STATUS, chunkId, status);
	}

	void serializeCstoclWriteStatus(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint32_t writeId, uint8_t status) {
		serializeMooseFsPacket(buffer, CSTOCL_WRITE_STATUS, chunkId, writeId, status);
	}
};

class LizardFsMessageSerializer : public MessageSerializer {
public:
	void serializePrefixOfCstoclReadData(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint32_t offset, uint32_t size) {
		cstocl::readData::serializePrefix(buffer, chunkId, offset, size);
	}

	void serializeCstoclReadStatus(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint8_t status) {
		cstocl::readStatus::serialize(buffer, chunkId, status);
	}

	void serializeCstoclWriteStatus(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint32_t writeId, uint8_t status) {
		cstocl::writeStatus::serialize(buffer, chunkId, writeId, status);
	}
};

MessageSerializer* MessageSerializer::getSerializer(PacketHeader::Type type) {
	sassert((type >= PacketHeader::kMinLizPacketType && type <= PacketHeader::kMaxLizPacketType)
			|| type <= PacketHeader::kMaxOldPacketType);
	if (type <= PacketHeader::kMaxOldPacketType) {
		static MooseFsMessageSerializer singleton;
		return &singleton;
	} else {
		static LizardFsMessageSerializer singleton;
		return &singleton;
	}
}

packetstruct* worker_create_detached_packet_with_output_buffer(
		const std::vector<uint8_t>& packetPrefix) {
	TRACETHIS();
	PacketHeader header;
	deserializePacketHeader(packetPrefix, header);
	uint32_t sizeOfWholePacket = PacketHeader::kSize + header.length;
	packetstruct* outPacket = new packetstruct();
	passert(outPacket);
	outPacket->outputBuffer.reset(new OutputBuffer(sizeOfWholePacket));
	if (outPacket->outputBuffer->copyIntoBuffer(packetPrefix) != (ssize_t)packetPrefix.size()) {
		delete outPacket;
		return nullptr;
	}
	return outPacket;
}

uint8_t* worker_get_packet_data(void *packet) {
	TRACETHIS();
	packetstruct *outpacket = (packetstruct*) packet;
	return outpacket->packet + 8;
}

void worker_delete_packet(void *packet) {
	TRACETHIS();
	packetstruct *outpacket = (packetstruct*) packet;
	free(outpacket->packet);
	delete outpacket;
}

void worker_attach_packet(csserventry *eptr, void *packet) {
	packetstruct *outpacket = (packetstruct*)packet;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
}

void* worker_preserve_inputpacket(csserventry *eptr) {
	TRACETHIS();
	void* ret;
	ret = eptr->inputpacket.packet;
	eptr->inputpacket.packet = NULL;
	return ret;
}

void worker_delete_preserved(void *p) {
	TRACETHIS();
	if (p) {
		free(p);
		p = NULL;
	}
}

void worker_create_attached_packet(csserventry *eptr, const std::vector<uint8_t>& packet) {
	TRACETHIS();
	packetstruct* outpacket = new packetstruct();
	passert(outpacket);
	outpacket->packet = (uint8_t*) malloc(packet.size());
	passert(outpacket->packet);
	memcpy(outpacket->packet, packet.data(), packet.size());
	outpacket->bytesleft = packet.size();
	outpacket->startptr = outpacket->packet;
	worker_attach_packet(eptr, outpacket);
}

uint8_t* worker_create_attached_packet(csserventry *eptr, uint32_t type, uint32_t size) {
	TRACETHIS();
	packetstruct *outpacket;
	uint8_t *ptr;
	uint32_t psize;

	outpacket = new packetstruct();
	passert(outpacket);
	psize = size + 8;
	outpacket->packet = (uint8_t*) malloc(psize);
	passert(outpacket->packet);
	outpacket->bytesleft = psize;
	ptr = outpacket->packet;
	put32bit(&ptr, type);
	put32bit(&ptr, size);
	outpacket->startptr = (uint8_t*) (outpacket->packet);
	outpacket->next = NULL;
	worker_attach_packet(eptr, outpacket);
	return ptr;
}

void worker_fwderror(csserventry *eptr) {
	TRACETHIS();
	sassert(eptr->messageSerializer != NULL);
	std::vector<uint8_t> buffer;
	uint8_t status = (eptr->state == CONNECTING ? LIZARDFS_ERROR_CANTCONNECT : LIZARDFS_ERROR_DISCONNECTED);
	eptr->messageSerializer->serializeCstoclWriteStatus(buffer, eptr->chunkid, 0, status);
	worker_create_attached_packet(eptr, buffer);
	eptr->state = WRITEFINISH;
}

// initialize connection to another CS
int worker_initconnect(csserventry *eptr) {
	TRACETHIS();
	int status;
	// TODO(msulikowski) If we want to use a ConnectionPool, this is the right place
	// to get a connection from it
	eptr->fwdsock = tcpsocket();
	if (eptr->fwdsock < 0) {
		lzfs_pretty_errlog(LOG_WARNING, "create socket, error");
		return -1;
	}
	if (tcpnonblock(eptr->fwdsock) < 0) {
		lzfs_pretty_errlog(LOG_WARNING, "set nonblock, error");
		tcpclose(eptr->fwdsock);
		eptr->fwdsock = -1;
		return -1;
	}
	status = tcpnumconnect(eptr->fwdsock, eptr->fwdServer.ip, eptr->fwdServer.port);
	if (status < 0) {
		lzfs_pretty_errlog(LOG_WARNING, "connect failed, error");
		tcpclose(eptr->fwdsock);
		eptr->fwdsock = -1;
		return -1;
	}
	if (status == 0) { // connected immediately
		tcpnodelay(eptr->fwdsock);
		eptr->state = WRITEINIT;
	} else {
		eptr->state = CONNECTING;
		eptr->connstart = eventloop_utime();
	}
	return 0;
}

void worker_retryconnect(csserventry *eptr) {
	TRACETHIS();
	tcpclose(eptr->fwdsock);
	eptr->fwdsock = -1;
	eptr->connretrycnt++;
	if (eptr->connretrycnt < CONNECT_RETRIES) {
		if (worker_initconnect(eptr) < 0) {
			worker_fwderror(eptr);
			return;
		}
	} else {
		worker_fwderror(eptr);
		return;
	}
}

void worker_check_nextpacket(csserventry *eptr);

// common - delayed close
void worker_delayed_close(uint8_t status, void *e) {
	TRACETHIS();
	csserventry *eptr = (csserventry*) e;
	if (eptr->wjobid > 0 && eptr->wjobwriteid == 0 && status == LIZARDFS_STATUS_OK) { // this was job_open
		eptr->chunkisopen = 1;
	} else if (eptr->rjobid > 0 && status == LIZARDFS_STATUS_OK) { //this could be job_open
		eptr->chunkisopen = 1;
	}
	if (eptr->chunkisopen) {
		job_close(eptr->workerJobPool, NULL, NULL, eptr->chunkid, eptr->chunkType);
		eptr->chunkisopen = 0;
	}
	eptr->state = CLOSED;
}

// bg reading

void worker_read_continue(csserventry *eptr);

void worker_read_finished(uint8_t status, void *e) {
	TRACETHIS();
	csserventry *eptr = (csserventry*) e;
	eptr->rjobid = 0;
	if (status == LIZARDFS_STATUS_OK) {
		eptr->todocnt--;
		eptr->chunkisopen = 1;
		if (eptr->todocnt == 0) {
			worker_read_continue(eptr);
		}
	} else {
		if (eptr->rpacket) {
			worker_delete_packet(eptr->rpacket);
			eptr->rpacket = NULL;
		}
		std::vector<uint8_t> buffer;
		eptr->messageSerializer->serializeCstoclReadStatus(buffer, eptr->chunkid, status);
		worker_create_attached_packet(eptr, buffer);
		if (eptr->chunkisopen) {
			job_close(eptr->workerJobPool, NULL, NULL, eptr->chunkid, eptr->chunkType);
			eptr->chunkisopen = 0;
		}
		eptr->state = IDLE; // after sending status even if there was an error it's possible to
		// receive new requests on the same connection
		LOG_AVG_STOP(eptr->readOperationTimer);
	}
}

void worker_send_finished(csserventry *eptr) {
	TRACETHIS();
	eptr->todocnt--;
	if (eptr->todocnt == 0) {
		worker_read_continue(eptr);
	}
}

void worker_read_continue(csserventry *eptr) {
	TRACETHIS2(eptr->offset, eptr->size);

	if (eptr->rpacket) {
		worker_attach_packet(eptr, eptr->rpacket);
		eptr->rpacket = NULL;
		eptr->todocnt++;
	}
	if (eptr->size == 0) { // everything has been read
		std::vector<uint8_t> buffer;
		eptr->messageSerializer->serializeCstoclReadStatus(buffer, eptr->chunkid, LIZARDFS_STATUS_OK);
		worker_create_attached_packet(eptr, buffer);
		sassert(eptr->chunkisopen);
		job_close(eptr->workerJobPool, NULL, NULL, eptr->chunkid, eptr->chunkType);
		eptr->chunkisopen = 0;
		eptr->state = IDLE; // no error - do not disconnect - go direct to the IDLE state, ready for requests on the same connection
		LOG_AVG_STOP(eptr->readOperationTimer);
	} else {
		const uint32_t totalRequestSize = eptr->size;
		const uint32_t thisPartOffset = eptr->offset % MFSBLOCKSIZE;
		const uint32_t thisPartSize = std::min<uint32_t>(
				totalRequestSize, MFSBLOCKSIZE - thisPartOffset);
		const uint16_t totalRequestBlocks =
				(totalRequestSize + thisPartOffset + MFSBLOCKSIZE - 1) / MFSBLOCKSIZE;
		std::vector<uint8_t> readDataPrefix;
		eptr->messageSerializer->serializePrefixOfCstoclReadData(readDataPrefix,
				eptr->chunkid, eptr->offset, thisPartSize);
		packetstruct* packet = worker_create_detached_packet_with_output_buffer(readDataPrefix);
		if (packet == nullptr) {
			eptr->state = CLOSE;
			return;
		}
		eptr->rpacket = (void*)packet;
		uint32_t readAheadBlocks = 0;
		uint32_t maxReadBehindBlocks = 0;
		if (!eptr->chunkisopen) {
			if (gHDDReadAhead.blocksToBeReadAhead() > 0) {
				readAheadBlocks = totalRequestBlocks + gHDDReadAhead.blocksToBeReadAhead();
			}
			// Try not to influence slow streams to much:
			maxReadBehindBlocks = std::min(totalRequestBlocks,
					gHDDReadAhead.maxBlocksToBeReadBehind());
		}
		eptr->rjobid = job_read(eptr->workerJobPool, worker_read_finished, eptr, eptr->chunkid,
				eptr->version, eptr->chunkType, eptr->offset, thisPartSize,
				maxReadBehindBlocks,
				readAheadBlocks,
				packet->outputBuffer.get(), !eptr->chunkisopen);
		if (eptr->rjobid == 0) {
			eptr->state = CLOSE;
			return;
		}
		eptr->todocnt++;
		eptr->offset += thisPartSize;
		eptr->size -= thisPartSize;
	}
}

void worker_ping(csserventry *eptr, const uint8_t *data, PacketHeader::Length length) {
	if (length != 4) {
		eptr->state = CLOSE;
		return;
	}

	uint32_t size;
	deserialize(data, length, size);
	worker_create_attached_packet(eptr, ANTOAN_PING_REPLY, size);
}

void worker_read_init(csserventry *eptr, const uint8_t *data,
		PacketHeader::Type type, PacketHeader::Length length) {
	TRACETHIS2(type, length);

	// Deserialize request
	sassert(type == LIZ_CLTOCS_READ || type == CLTOCS_READ);
	try {
		if (type == LIZ_CLTOCS_READ) {
			PacketVersion v;
			deserializePacketVersionNoHeader(data, length, v);
			if (v == cltocs::read::kECChunks) {
				cltocs::read::deserialize(data, length,
						eptr->chunkid,
						eptr->version,
						eptr->chunkType,
						eptr->offset,
						eptr->size);
			} else {
				legacy::ChunkPartType legacy_type;
				cltocs::read::deserialize(data, length,
						eptr->chunkid,
						eptr->version,
						legacy_type,
						eptr->offset,
						eptr->size);
				eptr->chunkType = legacy_type;
			}
		} else {
			deserializeAllMooseFsPacketDataNoHeader(data, length,
					eptr->chunkid,
					eptr->version,
					eptr->offset,
					eptr->size);
			eptr->chunkType = slice_traits::standard::ChunkPartType();
		}
		eptr->messageSerializer = MessageSerializer::getSerializer(type);
	} catch (IncorrectDeserializationException&) {
		syslog(LOG_NOTICE, "read_init: Cannot deserialize READ message (type:%"
				PRIX32 ", length:%" PRIu32 ")", type, length);
		eptr->state = CLOSE;
		return;
	}
	// Check if the request is valid
	std::vector<uint8_t> instantResponseBuffer;
	if (eptr->size == 0) {
		eptr->messageSerializer->serializeCstoclReadStatus(instantResponseBuffer,
				eptr->chunkid, LIZARDFS_STATUS_OK);
	} else if (eptr->size > MFSCHUNKSIZE) {
		eptr->messageSerializer->serializeCstoclReadStatus(instantResponseBuffer,
				eptr->chunkid, LIZARDFS_ERROR_WRONGSIZE);
	} else if (eptr->offset >= MFSCHUNKSIZE || eptr->offset + eptr->size > MFSCHUNKSIZE) {
		eptr->messageSerializer->serializeCstoclReadStatus(instantResponseBuffer,
				eptr->chunkid, LIZARDFS_ERROR_WRONGOFFSET);
	}
	if (!instantResponseBuffer.empty()) {
		worker_create_attached_packet(eptr, instantResponseBuffer);
		return;
	}
	// Process the request
	stats_hlopr++;
	eptr->state = READ;
	eptr->todocnt = 0;
	eptr->rjobid = 0;
	LOG_AVG_START0(eptr->readOperationTimer, "csserv_read");
	worker_read_continue(eptr);
}

void worker_prefetch(csserventry *eptr, const uint8_t *data, PacketHeader::Type type, PacketHeader::Length length) {
	sassert(type == LIZ_CLTOCS_PREFETCH);
	PacketVersion v;
	try {
		deserializePacketVersionNoHeader(data, length, v);
		if (v == cltocs::prefetch::kECChunks) {
			cltocs::prefetch::deserialize(data, length,
				eptr->chunkid,
				eptr->version,
				eptr->chunkType,
				eptr->offset,
				eptr->size);
		} else {
			legacy::ChunkPartType legacy_type;
			cltocs::prefetch::deserialize(data, length,
				eptr->chunkid,
				eptr->version,
				legacy_type,
				eptr->offset,
				eptr->size);
			eptr->chunkType = legacy_type;
		}
	} catch (IncorrectDeserializationException&) {
		syslog(LOG_NOTICE, "prefetch: Cannot deserialize PREFETCH message (type:%"
				PRIX32 ", length:%" PRIu32 ")", type, length);
		eptr->state = CLOSE;
		return;
	}
	// Start prefetching in background, don't wait for it to complete
	auto firstBlock = eptr->offset / MFSBLOCKSIZE;
	auto lastByte = eptr->offset + eptr->size - 1;
	auto lastBlock = lastByte / MFSBLOCKSIZE;
	auto nrOfBlocks = lastBlock - firstBlock + 1;
	job_prefetch(eptr->workerJobPool, eptr->chunkid, eptr->version, eptr->chunkType,
			firstBlock, nrOfBlocks);
}


// bg writing

void worker_write_finished(uint8_t status, void *e) {
	TRACETHIS();
	csserventry *eptr = (csserventry*) e;
	eptr->wjobid = 0;
	sassert(eptr->messageSerializer != NULL);
	if (status != LIZARDFS_STATUS_OK) {
		std::vector<uint8_t> buffer;
		eptr->messageSerializer->serializeCstoclWriteStatus(buffer,
				eptr->chunkid, eptr->wjobwriteid, status);
		worker_create_attached_packet(eptr, buffer);
		eptr->state = WRITEFINISH;
		return;
	}
	if (eptr->wjobwriteid == 0) {
		eptr->chunkisopen = 1;
	}
	if (eptr->state == WRITELAST) {
		std::vector<uint8_t> buffer;
		eptr->messageSerializer->serializeCstoclWriteStatus(buffer,
				eptr->chunkid, eptr->wjobwriteid, status);
		worker_create_attached_packet(eptr, buffer);
	} else {
		if (eptr->partiallyCompletedWrites.count(eptr->wjobwriteid) > 0) {
			// found - it means that it was added by status_receive, ie. next chunkserver from
			// a chain finished writing before our worker
			sassert(eptr->messageSerializer != NULL);
			std::vector<uint8_t> buffer;
			eptr->messageSerializer->serializeCstoclWriteStatus(buffer,
					eptr->chunkid, eptr->wjobwriteid, LIZARDFS_STATUS_OK);
			worker_create_attached_packet(eptr, buffer);
			eptr->partiallyCompletedWrites.erase(eptr->wjobwriteid);
		} else {
			// not found - so add it
			eptr->partiallyCompletedWrites.insert(eptr->wjobwriteid);
		}
	}
	worker_check_nextpacket(eptr);
}

void serializeCltocsWriteInit(std::vector<uint8_t>& buffer,
		uint64_t chunkId, uint32_t chunkVersion, ChunkPartType chunkType,
		const std::vector<ChunkTypeWithAddress>& chain, uint32_t target_version) {

	if (target_version >= kFirstECVersion) {
		cltocs::writeInit::serialize(buffer, chunkId, chunkVersion, chunkType, chain);
	} else if (target_version >= kFirstXorVersion) {
		assert((int)chunkType.getSliceType() < Goal::Slice::Type::kECFirst);
		std::vector<NetworkAddress> legacy_chain;
		legacy_chain.reserve(chain.size());
		for (const auto &entry : chain) {
			legacy_chain.push_back(entry.address);
		}
		cltocs::writeInit::serialize(buffer, chunkId, chunkVersion,
			(legacy::ChunkPartType)chunkType, legacy_chain);
	} else {
		assert(slice_traits::isStandard(chunkType));
		MooseFSVector<NetworkAddress> moose_chain;
		moose_chain.reserve(chain.size());
		for (const auto &entry : chain) {
			moose_chain.push_back(entry.address);
		}
		serializeMooseFsPacket(buffer, CLTOCS_WRITE, chunkId, chunkVersion, moose_chain);
	}
}

void worker_write_init(csserventry *eptr,
		const uint8_t *data, PacketHeader::Type type, PacketHeader::Length length) {
	TRACETHIS();
	std::vector<ChunkTypeWithAddress> chain;

	sassert(type == LIZ_CLTOCS_WRITE_INIT || type == CLTOCS_WRITE);
	try {
		if (type == LIZ_CLTOCS_WRITE_INIT) {
			PacketVersion v;
			deserializePacketVersionNoHeader(data, length, v);
			if (v == cltocs::writeInit::kECChunks) {
				cltocs::writeInit::deserialize(data, length,
					eptr->chunkid, eptr->version, eptr->chunkType, chain);
			} else {
				std::vector<NetworkAddress> legacy_chain;
				legacy::ChunkPartType legacy_type;
				cltocs::writeInit::deserialize(data, length,
					eptr->chunkid, eptr->version, legacy_type, legacy_chain);
				eptr->chunkType = legacy_type;
				for (const auto &address : legacy_chain) {
					chain.push_back(ChunkTypeWithAddress(address, eptr->chunkType, kFirstXorVersion));
				}
			}
		} else {
			MooseFSVector<NetworkAddress> mooseFSChain;
			deserializeAllMooseFsPacketDataNoHeader(data, length,
				eptr->chunkid, eptr->version, mooseFSChain);
			for (const auto &address : mooseFSChain) {
				chain.push_back(ChunkTypeWithAddress(address, slice_traits::standard::ChunkPartType(), kStdVersion));
			}
			eptr->chunkType = slice_traits::standard::ChunkPartType();
		}
		eptr->messageSerializer = MessageSerializer::getSerializer(type);
	} catch (IncorrectDeserializationException& ex) {
		syslog(LOG_NOTICE, "Received malformed WRITE_INIT message (length: %" PRIu32 ")", length);
		eptr->state = CLOSE;
		return;
	}

	if (!chain.empty()) {
		// Create a chain -- connect to the next chunkserver
		eptr->fwdServer = chain[0].address;
		uint32_t target_version = chain[0].chunkserver_version;
		chain.erase(chain.begin());
		serializeCltocsWriteInit(eptr->fwdinitpacket,
				eptr->chunkid, eptr->version, eptr->chunkType, chain, target_version);
		eptr->fwdstartptr = eptr->fwdinitpacket.data();
		eptr->fwdbytesleft = eptr->fwdinitpacket.size();
		eptr->connretrycnt = 0;
		if (worker_initconnect(eptr) < 0) {
			std::vector<uint8_t> buffer;
			eptr->messageSerializer->serializeCstoclWriteStatus(buffer,
					eptr->chunkid, 0, LIZARDFS_ERROR_CANTCONNECT);
			worker_create_attached_packet(eptr, buffer);
			eptr->state = WRITEFINISH;
			return;
		}
	} else {
		eptr->state = WRITELAST;
	}
	stats_hlopw++;
	eptr->wjobwriteid = 0;
	eptr->wjobid = job_open(eptr->workerJobPool, worker_write_finished, eptr, eptr->chunkid,
			eptr->chunkType);
}

void worker_write_data(csserventry *eptr,
		const uint8_t *data, PacketHeader::Type type, PacketHeader::Length length) {
	TRACETHIS();
	uint64_t chunkId;
	uint32_t writeId;
	uint16_t blocknum;
	uint32_t offset;
	uint32_t size;
	uint32_t crc;
	const uint8_t* dataToWrite;

	sassert(type == LIZ_CLTOCS_WRITE_DATA || type == CLTOCS_WRITE_DATA);
	try {
		const MessageSerializer *serializer = MessageSerializer::getSerializer(type);
		if (eptr->messageSerializer != serializer) {
			syslog(LOG_NOTICE, "Received WRITE_DATA message incompatible with WRITE_INIT");
			eptr->state = CLOSE;
			return;
		}
		if (type == LIZ_CLTOCS_WRITE_DATA) {
			cltocs::writeData::deserializePrefix(data, length,
					chunkId, writeId, blocknum, offset, size, crc);
			dataToWrite = data + cltocs::writeData::kPrefixSize;
		} else {
			uint16_t offset16;
			deserializeAllMooseFsPacketDataNoHeader(data, length,
				chunkId, writeId, blocknum, offset16, size, crc, dataToWrite);
			offset = offset16;
			sassert(eptr->chunkType == slice_traits::standard::ChunkPartType());
		}
	} catch (IncorrectDeserializationException&) {
		syslog(LOG_NOTICE, "Received malformed WRITE_DATA message (length: %" PRIu32 ")", length);
		eptr->state = CLOSE;
		return;
	}

	uint8_t status = LIZARDFS_STATUS_OK;
	uint32_t dataSize = data + length - dataToWrite;
	if (dataSize != size) {
		status = LIZARDFS_ERROR_WRONGSIZE;
	} else if (chunkId != eptr->chunkid) {
		status = LIZARDFS_ERROR_WRONGCHUNKID;
	}

	if (status != LIZARDFS_STATUS_OK) {
		std::vector<uint8_t> buffer;
		eptr->messageSerializer->serializeCstoclWriteStatus(buffer, chunkId, writeId, status);
		worker_create_attached_packet(eptr, buffer);
		eptr->state = WRITEFINISH;
		return;
	}
	if (eptr->wpacket) {
		worker_delete_preserved(eptr->wpacket);
	}
	eptr->wpacket = worker_preserve_inputpacket(eptr);
	eptr->wjobwriteid = writeId;
	eptr->wjobid = job_write(eptr->workerJobPool, worker_write_finished, eptr,
			chunkId, eptr->version, eptr->chunkType,
			blocknum, offset, size, crc, dataToWrite);
}

void worker_write_status(csserventry *eptr,
		const uint8_t *data, PacketHeader::Type type, PacketHeader::Length length) {
	TRACETHIS();
	uint64_t chunkId;
	uint32_t writeId;
	uint8_t status;

	sassert(type == LIZ_CSTOCL_WRITE_STATUS || type == CSTOCL_WRITE_STATUS);
	sassert(eptr->messageSerializer != NULL);
	try {
		const MessageSerializer *serializer = MessageSerializer::getSerializer(type);
		if (eptr->messageSerializer != serializer) {
			syslog(LOG_NOTICE, "Received WRITE_DATA message incompatible with WRITE_INIT");
			eptr->state = CLOSE;
			return;
		}
		if (type == LIZ_CSTOCL_WRITE_STATUS) {
			std::vector<uint8_t> message(data, data + length);
			cstocl::writeStatus::deserialize(message, chunkId, writeId, status);
		} else {
			deserializeAllMooseFsPacketDataNoHeader(data, length, chunkId, writeId, status);
			sassert(eptr->chunkType == slice_traits::standard::ChunkPartType());
		}
	} catch (IncorrectDeserializationException&) {
		syslog(LOG_NOTICE, "Received malformed WRITE_STATUS message (length: %" PRIu32 ")", length);
		eptr->state = CLOSE;
		return;
	}

	if (eptr->chunkid != chunkId) {
		status = LIZARDFS_ERROR_WRONGCHUNKID;
		writeId = 0;
	}

	if (status != LIZARDFS_STATUS_OK) {
		std::vector<uint8_t> buffer;
		eptr->messageSerializer->serializeCstoclWriteStatus(buffer, chunkId, writeId, status);
		worker_create_attached_packet(eptr, buffer);
		eptr->state = WRITEFINISH;
		return;
	}

	if (eptr->partiallyCompletedWrites.count(writeId) > 0) {
		// found - means it was added by write_finished
		std::vector<uint8_t> buffer;
		eptr->messageSerializer->serializeCstoclWriteStatus(buffer, chunkId, writeId, LIZARDFS_STATUS_OK);
		worker_create_attached_packet(eptr, buffer);
		eptr->partiallyCompletedWrites.erase(writeId);
	} else {
		// if not found then add record
		eptr->partiallyCompletedWrites.insert(writeId);
	}
}

void worker_write_end(csserventry *eptr, const uint8_t* data, uint32_t length) {
	uint64_t chunkId;
	eptr->messageSerializer = nullptr;
	try {
		cltocs::writeEnd::deserialize(data, length, chunkId);
	} catch (IncorrectDeserializationException&) {
		syslog(LOG_NOTICE,"Received malformed WRITE_END message (length: %" PRIu32 ")", length);
		eptr->state = WRITEFINISH;
		return;
	}
	if (chunkId != eptr->chunkid) {
		syslog(LOG_NOTICE,"Received malformed WRITE_END message "
				"(got chunkId=%016" PRIX64 ", expected %016" PRIX64 ")",
				chunkId, eptr->chunkid);
		eptr->state = WRITEFINISH;
		return;
	}
	if (eptr->wjobid > 0 || !eptr->partiallyCompletedWrites.empty() || eptr->outputhead != NULL) {
		/*
		 * WRITE_END received too early:
		 * eptr->wjobid > 0 -- hdd worker is working (writing some data)
		 * !eptr->partiallyCompletedWrites.empty() -- there are write tasks which have not been
		 *         acked by our hdd worker EX-or next chunkserver from a chain
		 * eptr->outputhead != NULL -- there is a status being send
		 */
		// TODO(msulikowski) temporary syslog message. May be useful until this code is fully tested
		syslog(LOG_NOTICE, "Received WRITE_END message too early");
		eptr->state = WRITEFINISH;
		return;
	}
	if (eptr->chunkisopen) {
		job_close(eptr->workerJobPool, NULL, NULL, eptr->chunkid, eptr->chunkType);
		eptr->chunkisopen = 0;
	}
	if (eptr->fwdsock > 0) {
		// TODO(msulikowski) if we want to use a ConnectionPool, this the right place to put the
		// connection to the pool.
		tcpclose(eptr->fwdsock);
		eptr->fwdsock = -1;
	}
	eptr->state = IDLE;
}

void worker_liz_get_chunk_blocks_finished_legacy(uint8_t status, void *extra) {
	TRACETHIS();
	csserventry *eptr = (csserventry*) extra;
	eptr->getBlocksJobId = 0;
	std::vector<uint8_t> buffer;
	cstocs::getChunkBlocksStatus::serialize(buffer, eptr->chunkid, eptr->version,
		(legacy::ChunkPartType)eptr->chunkType, eptr->getBlocksJobResult, status);
	worker_create_attached_packet(eptr, buffer);
	eptr->state = IDLE;
}

void worker_liz_get_chunk_blocks_finished(uint8_t status, void *extra) {
	TRACETHIS();
	csserventry *eptr = (csserventry*) extra;
	eptr->getBlocksJobId = 0;
	std::vector<uint8_t> buffer;
	cstocs::getChunkBlocksStatus::serialize(buffer, eptr->chunkid, eptr->version,
		eptr->chunkType, eptr->getBlocksJobResult, status);
	worker_create_attached_packet(eptr, buffer);
	eptr->state = IDLE;
}

void worker_get_chunk_blocks_finished(uint8_t status, void *extra) {
	TRACETHIS();
	csserventry *eptr = (csserventry*) extra;
	eptr->getBlocksJobId = 0;
	std::vector<uint8_t> buffer;
	serializeMooseFsPacket(buffer, CSTOCS_GET_CHUNK_BLOCKS_STATUS,
			eptr->chunkid, eptr->version, eptr->getBlocksJobResult, status);
	worker_create_attached_packet(eptr, buffer);
	eptr->state = IDLE;
}

void worker_liz_get_chunk_blocks(csserventry *eptr, const uint8_t *data, uint32_t length) {
	PacketVersion v;
	deserializePacketVersionNoHeader(data, length, v);
	if (v == cstocs::getChunkBlocks::kECChunks) {
		cstocs::getChunkBlocks::deserialize(data, length,
				eptr->chunkid, eptr->version, eptr->chunkType);

		eptr->getBlocksJobId = job_get_blocks(eptr->workerJobPool,
			worker_liz_get_chunk_blocks_finished, eptr, eptr->chunkid, eptr->version,
			eptr->chunkType, &(eptr->getBlocksJobResult));

	} else {
		legacy::ChunkPartType legacy_type;
		cstocs::getChunkBlocks::deserialize(data, length,
				eptr->chunkid, eptr->version, legacy_type);
		eptr->chunkType = legacy_type;

		eptr->getBlocksJobId = job_get_blocks(eptr->workerJobPool,
			worker_liz_get_chunk_blocks_finished_legacy, eptr, eptr->chunkid, eptr->version,
			eptr->chunkType, &(eptr->getBlocksJobResult));
	}
	eptr->state = GET_BLOCK;
}

void worker_get_chunk_blocks(csserventry *eptr, const uint8_t *data,
		uint32_t length) {
	deserializeAllMooseFsPacketDataNoHeader(data, length, eptr->chunkid, eptr->version);
	eptr->chunkType = slice_traits::standard::ChunkPartType();
	eptr->getBlocksJobId = job_get_blocks(eptr->workerJobPool,
			worker_get_chunk_blocks_finished, eptr, eptr->chunkid, eptr->version,
			eptr->chunkType, &(eptr->getBlocksJobResult));
	eptr->state = GET_BLOCK;
}

/* IDLE operations */

void worker_hdd_list_v1(csserventry *eptr, const uint8_t *data,
		uint32_t length) {
	TRACETHIS();
	uint32_t l;
	uint8_t *ptr;

	(void) data;
	if (length != 0) {
		syslog(LOG_NOTICE,"CLTOCS_HDD_LIST(1) - wrong size (%" PRIu32 "/0)",length);
		eptr->state = CLOSE;
		return;
	}
	l = hdd_diskinfo_v1_size(); // lock
	ptr = worker_create_attached_packet(eptr, CSTOCL_HDD_LIST_V1, l);
	hdd_diskinfo_v1_data(ptr); // unlock
}

void worker_hdd_list_v2(csserventry *eptr, const uint8_t *data,
		uint32_t length) {
	TRACETHIS();
	uint32_t l;
	uint8_t *ptr;

	(void) data;
	if (length != 0) {
		syslog(LOG_NOTICE,"CLTOCS_HDD_LIST_V2 - wrong size (%" PRIu32 "/0)",length);
		eptr->state = CLOSE;
		return;
	}
	l = hdd_diskinfo_v2_size(); // lock
	ptr = worker_create_attached_packet(eptr, CSTOCL_HDD_LIST_V2, l);
	hdd_diskinfo_v2_data(ptr); // unlock
}

void worker_chart(csserventry *eptr, const uint8_t *data, uint32_t length) {
	TRACETHIS();
	uint32_t chartid;
	uint8_t *ptr;
	uint32_t l;

	if (length != 4) {
		syslog(LOG_NOTICE,"CLTOAN_CHART - wrong size (%" PRIu32 "/4)",length);
		eptr->state = CLOSE;
		return;
	}
	chartid = get32bit(&data);
	if(chartid <= CHARTS_CSV_CHARTID_BASE) {
		l = charts_make_png(chartid);
		ptr = worker_create_attached_packet(eptr, ANTOCL_CHART, l);
		if (l > 0) {
			charts_get_png(ptr);
		}
	} else {
		l = charts_make_csv(chartid % CHARTS_CSV_CHARTID_BASE);
		ptr = worker_create_attached_packet(eptr,ANTOCL_CHART,l);
		if (l>0) {
			charts_get_csv(ptr);
		}
	}
}

void worker_chart_data(csserventry *eptr, const uint8_t *data, uint32_t length) {
	TRACETHIS();
	uint32_t chartid;
	uint8_t *ptr;
	uint32_t l;

	if (length != 4) {
		syslog(LOG_NOTICE,"CLTOAN_CHART_DATA - wrong size (%" PRIu32 "/4)",length);
		eptr->state = CLOSE;
		return;
	}
	chartid = get32bit(&data);
	l = charts_datasize(chartid);
	ptr = worker_create_attached_packet(eptr, ANTOCL_CHART_DATA, l);
	if (l > 0) {
		charts_makedata(ptr, chartid);
	}
}

void worker_test_chunk(csserventry *eptr, const uint8_t *data, uint32_t length) {
	try {
		PacketVersion v;
		deserializePacketVersionNoHeader(data, length, v);
		ChunkWithVersionAndType chunk;
		if (v == cltocs::testChunk::kECChunks) {
			cltocs::testChunk::deserialize(data, length, chunk.id, chunk.version, chunk.type);
		} else {
			legacy::ChunkPartType legacy_type;
			cltocs::testChunk::deserialize(data, length, chunk.id, chunk.version, legacy_type);
			chunk.type = legacy_type;
		}
		hdd_test_chunk(chunk);
	} catch (IncorrectDeserializationException &e) {
		syslog(LOG_NOTICE, "LIZ_CLTOCS_TEST_CHUNK - bad packet: %s (length: %" PRIu32 ")",
				e.what(), length);
		eptr->state = CLOSE;
		return;
	}
}


void worker_outputcheck(csserventry *eptr) {
	TRACETHIS();
	if (eptr->state == READ) {
		worker_send_finished(eptr);
	}
}

void worker_close(csserventry *eptr) {
	TRACETHIS();
	if (eptr->rjobid > 0) {
		job_pool_disable_job(eptr->workerJobPool, eptr->rjobid);
		job_pool_change_callback(eptr->workerJobPool, eptr->rjobid, worker_delayed_close, eptr);
		eptr->state = CLOSEWAIT;
	} else if (eptr->wjobid > 0) {
		job_pool_disable_job(eptr->workerJobPool, eptr->wjobid);
		job_pool_change_callback(eptr->workerJobPool, eptr->wjobid, worker_delayed_close, eptr);
		eptr->state = CLOSEWAIT;
	} else if (eptr->getBlocksJobId > 0) {
		job_pool_disable_job(eptr->workerJobPool, eptr->getBlocksJobId);
		job_pool_change_callback(eptr->workerJobPool, eptr->getBlocksJobId, worker_delayed_close, eptr);
		eptr->state = CLOSEWAIT;
	} else {
		if (eptr->chunkisopen) {
			job_close(eptr->workerJobPool, NULL, NULL, eptr->chunkid, eptr->chunkType);
			eptr->chunkisopen = 0;
		}
		eptr->state = CLOSED;
	}
}

void worker_gotpacket(csserventry *eptr, uint32_t type, const uint8_t *data, uint32_t length) {
	TRACETHIS();
//      syslog(LOG_NOTICE,"packet %u:%u",type,length);
	if (type == ANTOAN_NOP) {
		return;
	}
	if (type == ANTOAN_UNKNOWN_COMMAND) { // for future use
		return;
	}
	if (type == ANTOAN_BAD_COMMAND_SIZE) { // for future use
		return;
	}
	if (eptr->state == IDLE) {
		switch (type) {
		case ANTOAN_PING:
			worker_ping(eptr, data, length);
			break;
		case CLTOCS_READ:
		case LIZ_CLTOCS_READ:
			worker_read_init(eptr, data, type, length);
			break;
		case LIZ_CLTOCS_PREFETCH:
			worker_prefetch(eptr, data, type, length);
			break;
		case CLTOCS_WRITE:
		case LIZ_CLTOCS_WRITE_INIT:
			worker_write_init(eptr, data, type, length);
			break;
		case CSTOCS_GET_CHUNK_BLOCKS:
			worker_get_chunk_blocks(eptr, data, length);
			break;
		case LIZ_CSTOCS_GET_CHUNK_BLOCKS:
			worker_liz_get_chunk_blocks(eptr, data, length);
			break;
		case CLTOCS_HDD_LIST_V1:
			worker_hdd_list_v1(eptr, data, length);
			break;
		case CLTOCS_HDD_LIST_V2:
			worker_hdd_list_v2(eptr, data, length);
			break;
		case CLTOAN_CHART:
			worker_chart(eptr, data, length);
			break;
		case CLTOAN_CHART_DATA:
			worker_chart_data(eptr, data, length);
			break;
		case LIZ_CLTOCS_TEST_CHUNK:
			worker_test_chunk(eptr, data, length);
			break;
		default:
			syslog(LOG_NOTICE, "Got invalid message in IDLE state (type:%" PRIu32 ")",type);
			eptr->state = CLOSE;
			break;
		}
	} else if (eptr->state == WRITELAST) {
		switch (type) {
		case CLTOCS_WRITE_DATA:
		case LIZ_CLTOCS_WRITE_DATA:
			worker_write_data(eptr, data, type, length);
			break;
		case LIZ_CLTOCS_WRITE_END:
			worker_write_end(eptr, data, length);
			break;
		default:
			syslog(LOG_NOTICE, "Got invalid message in WRITELAST state (type:%" PRIu32 ")",type);
			eptr->state = CLOSE;
			break;
		}
	} else if (eptr->state == WRITEFWD) {
		switch (type) {
		case CLTOCS_WRITE_DATA:
		case LIZ_CLTOCS_WRITE_DATA:
			worker_write_data(eptr, data, type, length);
			break;
		case CSTOCL_WRITE_STATUS:
		case LIZ_CSTOCL_WRITE_STATUS:
			worker_write_status(eptr, data, type, length);
			break;
		case LIZ_CLTOCS_WRITE_END:
			worker_write_end(eptr, data, length);
			break;
		default:
			syslog(LOG_NOTICE, "Got invalid message in WRITEFWD state (type:%" PRIu32 ")",type);
			eptr->state = CLOSE;
			break;
		}
	} else if (eptr->state == WRITEFINISH) {
		switch (type) {
		case CLTOCS_WRITE_DATA:
		case LIZ_CLTOCS_WRITE_DATA:
		case LIZ_CLTOCS_WRITE_END:
			return;
		default:
			syslog(LOG_NOTICE, "Got invalid message in WRITEFINISH state (type:%" PRIu32 ")",type);
			eptr->state = CLOSE;
		}
	} else {
		syslog(LOG_NOTICE, "Got invalid message (type:%" PRIu32 ")",type);
		eptr->state = CLOSE;
	}
}

void worker_check_nextpacket(csserventry *eptr) {
	TRACETHIS();
	uint32_t type, size;
	const uint8_t *ptr;
	if (eptr->state == WRITEFWD) {
		if (eptr->mode == DATA && eptr->inputpacket.bytesleft == 0 && eptr->fwdbytesleft == 0) {
			ptr = eptr->hdrbuff;
			type = get32bit(&ptr);
			size = get32bit(&ptr);

			eptr->mode = HEADER;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;

			worker_gotpacket(eptr, type, eptr->inputpacket.packet + 8, size);

			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet = NULL;
		}
	} else {
		if (eptr->mode == DATA && eptr->inputpacket.bytesleft == 0) {
			ptr = eptr->hdrbuff;
			type = get32bit(&ptr);
			size = get32bit(&ptr);

			eptr->mode = HEADER;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;

			worker_gotpacket(eptr, type, eptr->inputpacket.packet, size);

			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet = NULL;
		}
	}
}

void worker_fwdconnected(csserventry *eptr) {
	TRACETHIS();
	int status;
	status = tcpgetstatus(eptr->fwdsock);
	if (status) {
		lzfs_silent_errlog(LOG_WARNING, "connection failed, error");
		worker_fwderror(eptr);
		return;
	}
	tcpnodelay(eptr->fwdsock);
	eptr->state = WRITEINIT;
}

void worker_fwdread(csserventry *eptr) {
	TRACETHIS();
	int32_t i;
	uint32_t type, size;
	const uint8_t *ptr;
	if (eptr->fwdmode == HEADER) {
		i = read(eptr->fwdsock, eptr->fwdinputpacket.startptr,
				eptr->fwdinputpacket.bytesleft);
		if (i == 0) {
//                      syslog(LOG_NOTICE,"(fwdread) connection closed");
			worker_fwderror(eptr);
			return;
		}
		if (i < 0) {
			if (errno != EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE, "(fwdread) read error");
				worker_fwderror(eptr);
			}
			return;
		}
		stats_bytesin += i;
		eptr->fwdinputpacket.startptr += i;
		eptr->fwdinputpacket.bytesleft -= i;
		if (eptr->fwdinputpacket.bytesleft > 0) {
			return;
		}
		ptr = eptr->fwdhdrbuff + 4;
		size = get32bit(&ptr);
		if (size > MaxPacketSize) {
			syslog(LOG_WARNING,"(fwdread) packet too long (%" PRIu32 "/%u)",size,MaxPacketSize);
			worker_fwderror(eptr);
			return;
		}
		if (size > 0) {
			eptr->fwdinputpacket.packet = (uint8_t*) malloc(size);
			passert(eptr->fwdinputpacket.packet);
			eptr->fwdinputpacket.startptr = eptr->fwdinputpacket.packet;
		}
		eptr->fwdinputpacket.bytesleft = size;
		eptr->fwdmode = DATA;
	}
	if (eptr->fwdmode == DATA) {
		if (eptr->fwdinputpacket.bytesleft > 0) {
			i = read(eptr->fwdsock, eptr->fwdinputpacket.startptr,
					eptr->fwdinputpacket.bytesleft);
			if (i == 0) {
//                              syslog(LOG_NOTICE,"(fwdread) connection closed");
				worker_fwderror(eptr);
				return;
			}
			if (i < 0) {
				if (errno != EAGAIN) {
					lzfs_silent_errlog(LOG_NOTICE, "(fwdread) read error");
					worker_fwderror(eptr);
				}
				return;
			}
			stats_bytesin += i;
			eptr->fwdinputpacket.startptr += i;
			eptr->fwdinputpacket.bytesleft -= i;
			if (eptr->fwdinputpacket.bytesleft > 0) {
				return;
			}
		}
		ptr = eptr->fwdhdrbuff;
		type = get32bit(&ptr);
		size = get32bit(&ptr);

		eptr->fwdmode = HEADER;
		eptr->fwdinputpacket.bytesleft = 8;
		eptr->fwdinputpacket.startptr = eptr->fwdhdrbuff;

		worker_gotpacket(eptr, type, eptr->fwdinputpacket.packet, size);

		if (eptr->fwdinputpacket.packet) {
			free(eptr->fwdinputpacket.packet);
		}
		eptr->fwdinputpacket.packet = NULL;
	}
}

void worker_fwdwrite(csserventry *eptr) {
	TRACETHIS();
	int32_t i;
	if (eptr->fwdbytesleft > 0) {
		i = write(eptr->fwdsock, eptr->fwdstartptr, eptr->fwdbytesleft);
		if (i == 0) {
//                      syslog(LOG_NOTICE,"(fwdwrite) connection closed");
			worker_fwderror(eptr);
			return;
		}
		if (i < 0) {
			if (errno != EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE, "(fwdwrite) write error");
				worker_fwderror(eptr);
			}
			return;
		}
		stats_bytesout += i;
		eptr->fwdstartptr += i;
		eptr->fwdbytesleft -= i;
	}
	if (eptr->fwdbytesleft == 0) {
		eptr->fwdinitpacket.clear();
		eptr->fwdstartptr = NULL;
		eptr->fwdmode = HEADER;
		eptr->fwdinputpacket.bytesleft = 8;
		eptr->fwdinputpacket.startptr = eptr->fwdhdrbuff;
		eptr->fwdinputpacket.packet = NULL;
		eptr->state = WRITEFWD;
	}
}

void worker_forward(csserventry *eptr) {
	TRACETHIS();
	int32_t i;
	if (eptr->mode == HEADER) {
		i = read(eptr->sock, eptr->inputpacket.startptr, eptr->inputpacket.bytesleft);
		if (i == 0) {
			eptr->state = CLOSE;
			return;
		}
		if (i < 0) {
			if (errno != EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE, "(forward) read error");
				eptr->state = CLOSE;
			}
			return;
		}
		stats_bytesin += i;
		eptr->inputpacket.startptr += i;
		eptr->inputpacket.bytesleft -= i;
		if (eptr->inputpacket.bytesleft > 0) {
			return;
		}
		PacketHeader header;
		try {
			deserializePacketHeader(eptr->hdrbuff, sizeof(eptr->hdrbuff), header);
		} catch (IncorrectDeserializationException&) {
			syslog(LOG_WARNING, "(forward) Received malformed network packet");
			eptr->state = CLOSE;
			return;
		}
		if (header.length > MaxPacketSize) {
			syslog(LOG_WARNING,"(forward) packet too long (%" PRIu32 "/%u)",
					header.length, MaxPacketSize);
			eptr->state = CLOSE;
			return;
		}
		uint32_t totalPacketLength = PacketHeader::kSize + header.length;
		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		eptr->inputpacket.packet = static_cast<uint8_t*>(malloc(totalPacketLength));
		passert(eptr->inputpacket.packet);
		memcpy(eptr->inputpacket.packet, eptr->hdrbuff, PacketHeader::kSize);
		eptr->inputpacket.bytesleft = header.length;
		eptr->inputpacket.startptr = eptr->inputpacket.packet + PacketHeader::kSize;
		if (header.type == CLTOCS_WRITE_DATA
				|| header.type == LIZ_CLTOCS_WRITE_DATA
				|| header.type == LIZ_CLTOCS_WRITE_END) {
			eptr->fwdbytesleft = 8;
			eptr->fwdstartptr = eptr->inputpacket.packet;
		}
		eptr->mode = DATA;
	}
	if (eptr->inputpacket.bytesleft > 0) {
		i = read(eptr->sock, eptr->inputpacket.startptr, eptr->inputpacket.bytesleft);
		if (i == 0) {
			eptr->state = CLOSE;
			return;
		}
		if (i < 0) {
			if (errno != EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE, "(forward) read error");
				eptr->state = CLOSE;
			}
			return;
		}
		stats_bytesin += i;
		eptr->inputpacket.startptr += i;
		eptr->inputpacket.bytesleft -= i;
		if (eptr->fwdstartptr != NULL) {
			eptr->fwdbytesleft += i;
		}
	}
	if (eptr->fwdbytesleft > 0) {
		sassert(eptr->fwdstartptr != NULL);
		i = write(eptr->fwdsock, eptr->fwdstartptr, eptr->fwdbytesleft);
		if (i == 0) {
			worker_fwderror(eptr);
			return;
		}
		if (i < 0) {
			if (errno != EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE, "(forward) write error");
				worker_fwderror(eptr);
			}
			return;
		}
		stats_bytesout += i;
		eptr->fwdstartptr += i;
		eptr->fwdbytesleft -= i;
	}
	if (eptr->inputpacket.bytesleft == 0 && eptr->fwdbytesleft == 0 && eptr->wjobid == 0) {
		PacketHeader header;
		try {
			deserializePacketHeader(eptr->hdrbuff, sizeof(eptr->hdrbuff), header);
		} catch (IncorrectDeserializationException&) {
			syslog(LOG_WARNING, "(forward) Received malformed network packet");
			eptr->state = CLOSE;
			return;
		}
		eptr->mode = HEADER;
		eptr->inputpacket.bytesleft = 8;
		eptr->inputpacket.startptr = eptr->hdrbuff;

		uint8_t* packetData = eptr->inputpacket.packet + PacketHeader::kSize;
		worker_gotpacket(eptr, header.type, packetData, header.length);
		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		eptr->inputpacket.packet = NULL;
		eptr->fwdstartptr = NULL;
	}
}

void worker_read(csserventry *eptr) {
	TRACETHIS();
	int32_t i;
	uint32_t type, size;
	const uint8_t *ptr;

	if (eptr->mode == HEADER) {
		sassert(eptr->inputpacket.startptr + eptr->inputpacket.bytesleft == eptr->hdrbuff + 8);
		i = read(eptr->sock, eptr->inputpacket.startptr,
				eptr->inputpacket.bytesleft);
		if (i == 0) {
//                      syslog(LOG_NOTICE,"(read) connection closed");
			eptr->state = CLOSE;
			return;
		}
		if (i < 0) {
			if (errno != EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE, "(read) read error");
				eptr->state = CLOSE;
			}
			return;
		}
		stats_bytesin += i;
		eptr->inputpacket.startptr += i;
		eptr->inputpacket.bytesleft -= i;

		if (eptr->inputpacket.bytesleft > 0) {
			return;
		}

		ptr = eptr->hdrbuff + 4;
		size = get32bit(&ptr);

		if (size > 0) {
			if (size > MaxPacketSize) {
				syslog(LOG_WARNING,"(read) packet too long (%" PRIu32 "/%u)",size,MaxPacketSize);
				eptr->state = CLOSE;
				return;
			}
			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet = (uint8_t*) malloc(size);
			passert(eptr->inputpacket.packet);
			eptr->inputpacket.startptr = eptr->inputpacket.packet;
		}
		eptr->inputpacket.bytesleft = size;
		eptr->mode = DATA;
	}
	if (eptr->mode == DATA) {
		if (eptr->inputpacket.bytesleft > 0) {
			i = read(eptr->sock, eptr->inputpacket.startptr,
					eptr->inputpacket.bytesleft);
			if (i == 0) {
//                              syslog(LOG_NOTICE,"(read) connection closed");
				eptr->state = CLOSE;
				return;
			}
			if (i < 0) {
				if (errno != EAGAIN) {
					lzfs_silent_errlog(LOG_NOTICE, "(read) read error");
					eptr->state = CLOSE;
				}
				return;
			}
			stats_bytesin += i;
			eptr->inputpacket.startptr += i;
			eptr->inputpacket.bytesleft -= i;

			if (eptr->inputpacket.bytesleft > 0) {
				return;
			}
		}
		if (eptr->wjobid == 0) {
			ptr = eptr->hdrbuff;
			type = get32bit(&ptr);
			size = get32bit(&ptr);

			eptr->mode = HEADER;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;

			worker_gotpacket(eptr, type, eptr->inputpacket.packet, size);

			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet = NULL;
		}
	}
}

void worker_write(csserventry *eptr) {
	TRACETHIS();
	packetstruct *pack;
	int32_t i;
	for (;;) {
		pack = eptr->outputhead;
		if (pack == NULL) {
			return;
		}
		if (pack->outputBuffer) {
			size_t bytesInBufferBefore = pack->outputBuffer->bytesInABuffer();
			OutputBuffer::WriteStatus ret = pack->outputBuffer->writeOutToAFileDescriptor(eptr->sock);
			size_t bytesInBufferAfter = pack->outputBuffer->bytesInABuffer();
			massert(bytesInBufferAfter <= bytesInBufferBefore,
					"New bytes in pack->outputBuffer after sending some data");
			stats_bytesout += (bytesInBufferBefore - bytesInBufferAfter);
			if (ret == OutputBuffer::WRITE_ERROR) {
				lzfs_silent_errlog(LOG_NOTICE, "(write) write error");
				eptr->state = CLOSE;
				return;
			} else if (ret == OutputBuffer::WRITE_AGAIN) {
				return;
			}
		} else {
			i = write(eptr->sock, pack->startptr, pack->bytesleft);
			if (i == 0) {
//                              syslog(LOG_NOTICE,"(write) connection closed");
				eptr->state = CLOSE;
				return;
			}
			if (i < 0) {
				if (errno != EAGAIN) {
					lzfs_silent_errlog(LOG_NOTICE, "(write) write error");
					eptr->state = CLOSE;
				}
				return;
			}
			stats_bytesout += i;
			pack->startptr += i;
			pack->bytesleft -= i;
			if (pack->bytesleft > 0) {
				return;
			}
		}
		// packet has been sent
		free(pack->packet);
		eptr->outputhead = pack->next;
		if (eptr->outputhead == NULL) {
			eptr->outputtail = &(eptr->outputhead);
		}
		delete pack;
		worker_outputcheck(eptr);
	}
}

NetworkWorkerThread::NetworkWorkerThread(uint32_t nrOfBgjobsWorkers, uint32_t bgjobsCount)
		: doTerminate(false) {
	TRACETHIS();
	eassert(pipe(notify_pipe) != -1);
#ifdef F_SETPIPE_SZ
	eassert(fcntl(notify_pipe[1], F_SETPIPE_SZ, 4096*32));
#endif
	bgJobPool_ = job_pool_new(nrOfBgjobsWorkers, bgjobsCount, &bgJobPoolWakeUpFd_);
}

void NetworkWorkerThread::operator()() {
	TRACETHIS();
	while (!doTerminate) {
		preparePollFds();
		int i = poll(pdesc.data(), pdesc.size(), 50);
		if (i < 0) {
			if (errno == EAGAIN) {
				syslog(LOG_WARNING, "poll returned EAGAIN");
				usleep(100000);
				continue;
			}
			if (errno != EINTR) {
				syslog(LOG_WARNING, "poll error: %s", strerr(errno));
				break;
			}
		} else {
			if ((pdesc[0].revents) & POLLIN) {
				uint8_t notifyByte;
				eassert(read(pdesc[0].fd, &notifyByte, 1) == 1);
			}
		}
		servePoll();
	}
	this->terminate();
}

void NetworkWorkerThread::terminate() {
	TRACETHIS();
	job_pool_delete(bgJobPool_);
	std::unique_lock<std::mutex> lock(csservheadLock);
	while (!csservEntries.empty()) {
		auto& entry = csservEntries.back();
		if (entry.chunkisopen) {
			hdd_close(entry.chunkid, entry.chunkType);
		}
		tcpclose(entry.sock);
		if (entry.fwdsock >= 0) {
			tcpclose(entry.fwdsock);
		}
		if (entry.inputpacket.packet) {
			free(entry.inputpacket.packet);
		}
		if (entry.wpacket) {
			worker_delete_preserved(entry.wpacket);
		}
		if (entry.fwdinputpacket.packet) {
			free(entry.fwdinputpacket.packet);
		}
		packetstruct* pptr = entry.outputhead;
		while (pptr) {
			if (pptr->packet) {
				free(pptr->packet);
			}
			packetstruct* paptr = pptr;
			pptr = pptr->next;
			delete paptr;
		}
		csservEntries.pop_back();
	}
}

void NetworkWorkerThread::preparePollFds() {
	LOG_AVG_TILL_END_OF_SCOPE0("preparePollFds");
	TRACETHIS();
	pdesc.clear();
	pdesc.emplace_back();
	pdesc.back().fd = notify_pipe[0];
	pdesc.back().events = POLLIN;
	pdesc.emplace_back();
	pdesc.back().fd = bgJobPoolWakeUpFd_;
	pdesc.back().events = POLLIN;
	sassert(JOB_FD_PDESC_POS == (pdesc.size() - 1));

	std::unique_lock<std::mutex> lock(csservheadLock);
	for (auto& entry : csservEntries) {
		entry.pdescpos = -1;
		entry.fwdpdescpos = -1;
		switch (entry.state) {
			case IDLE:
			case READ:
			case GET_BLOCK:
			case WRITELAST:
				pdesc.emplace_back();
				pdesc.back().fd = entry.sock;
				pdesc.back().events = 0;
				entry.pdescpos = pdesc.size() - 1;
				if (entry.inputpacket.bytesleft > 0) {
					pdesc.back().events |= POLLIN;
				}
				if (entry.outputhead != NULL) {
					pdesc.back().events |= POLLOUT;
				}
				break;
			case CONNECTING:
				pdesc.emplace_back();
				pdesc.back().fd = entry.fwdsock;
				pdesc.back().events = POLLOUT;
				entry.fwdpdescpos = pdesc.size() - 1;
				break;
			case WRITEINIT:
				if (entry.fwdbytesleft > 0) {
					pdesc.emplace_back();
					pdesc.back().fd = entry.fwdsock;
					pdesc.back().events = POLLOUT;
					entry.fwdpdescpos = pdesc.size() - 1;
				}
				break;
			case WRITEFWD:
				pdesc.emplace_back();
				pdesc.back().fd = entry.fwdsock;
				pdesc.back().events = POLLIN;
				entry.fwdpdescpos = pdesc.size() - 1;
				if (entry.fwdbytesleft > 0) {
					pdesc.back().events |= POLLOUT;
				}

				pdesc.emplace_back();
				pdesc.back().fd = entry.sock;
				pdesc.back().events = 0;
				entry.pdescpos = pdesc.size() - 1;
				if (entry.inputpacket.bytesleft > 0) {
					pdesc.back().events |= POLLIN;
				}
				if (entry.outputhead != NULL) {
					pdesc.back().events |= POLLOUT;
				}
				break;
			case WRITEFINISH:
				if (entry.outputhead != NULL) {
					pdesc.emplace_back();
					pdesc.back().fd = entry.sock;
					pdesc.back().events = POLLOUT;
					entry.pdescpos = pdesc.size() - 1;
				}
				break;
		}
	}
}

void NetworkWorkerThread::servePoll() {
	LOG_AVG_TILL_END_OF_SCOPE0("servePoll");
	TRACETHIS();
	uint32_t now = eventloop_time();
	uint64_t usecnow = eventloop_utime();
	uint32_t jobscnt;
	uint8_t lstate;

	if (pdesc[JOB_FD_PDESC_POS].revents & POLLIN) {
		job_pool_check_jobs(bgJobPool_);
	}
	std::unique_lock<std::mutex> lock(csservheadLock);
	for (auto& entry : csservEntries) {
		csserventry* eptr = &entry;
		if (entry.pdescpos >= 0
				&& (pdesc[entry.pdescpos].revents & (POLLERR | POLLHUP))) {
			entry.state = CLOSE;
		} else if (entry.fwdpdescpos >= 0
				&& (pdesc[entry.fwdpdescpos].revents & (POLLERR | POLLHUP))) {
			worker_fwderror(eptr);
		}
		lstate = entry.state;
		if (lstate == IDLE || lstate == READ || lstate == WRITELAST || lstate == WRITEFINISH
				|| lstate == GET_BLOCK) {
			if (entry.pdescpos >= 0 && (pdesc[entry.pdescpos].revents & POLLIN)) {
				entry.activity = now;
				worker_read(eptr);
			}
			if (entry.pdescpos >= 0 && (pdesc[entry.pdescpos].revents & POLLOUT)
					&& entry.state == lstate) {
				entry.activity = now;
				worker_write(eptr);
			}
		} else if (lstate == CONNECTING && entry.fwdpdescpos >= 0
				&& (pdesc[entry.fwdpdescpos].revents & POLLOUT)) { // FD_ISSET(entry.fwdsock,wset)) {
			entry.activity = now;
			worker_fwdconnected(eptr);
			if (entry.state == WRITEINIT) {
				worker_fwdwrite(eptr); // after connect likely some data can be send
			}
			if (entry.state == WRITEFWD) {
				worker_forward(eptr); // and also some data can be forwarded
			}
		} else if (entry.state == WRITEINIT && entry.fwdpdescpos >= 0
				&& (pdesc[entry.fwdpdescpos].revents & POLLOUT)) { // FD_ISSET(entry.fwdsock,wset)) {
			entry.activity = now;
			worker_fwdwrite(eptr); // after sending init packet
			if (entry.state == WRITEFWD) {
				worker_forward(eptr); // likely some data can be forwarded
			}
		} else if (entry.state == WRITEFWD) {
			if ((entry.pdescpos >= 0 && (pdesc[entry.pdescpos].revents & POLLIN))
					|| (entry.fwdpdescpos >= 0
							&& (pdesc[entry.fwdpdescpos].revents & POLLOUT))) {
				entry.activity = now;
				worker_forward(eptr);
			}
			if (entry.fwdpdescpos >= 0 && (pdesc[entry.fwdpdescpos].revents & POLLIN)
					&& entry.state == lstate) {
				entry.activity = now;
				worker_fwdread(eptr);
			}
			if (entry.pdescpos >= 0 && (pdesc[entry.pdescpos].revents & POLLOUT)
					&& entry.state == lstate) {
				entry.activity = now;
				worker_write(eptr);
			}
		}
		if (entry.state == WRITEFINISH && entry.outputhead == NULL) {
			entry.state = CLOSE;
		}
		if (entry.state == CONNECTING
				&& entry.connstart + CONNECT_TIMEOUT(entry.connretrycnt) < usecnow) {
			worker_retryconnect(eptr);
		}
		if (entry.state != CLOSE && entry.state != CLOSEWAIT
				&& entry.state != CLOSED && entry.activity + CSSERV_TIMEOUT < now) {
			// Close connection if inactive for more than CSSERV_TIMEOUT seconds
			entry.state = CLOSE;
		}
		if (entry.state == CLOSE) {
			worker_close(eptr);
		}
	}

	jobscnt = job_pool_jobs_count(bgJobPool_);
//      // Lock free stats_maxjobscnt = max(stats_maxjobscnt, jobscnt), but I don't trust myself :(...
//      uint32_t expected_value = stats_maxjobscnt;
//      while (jobscnt > expected_value
//                      && !stats_maxjobscnt.compare_exchange_strong(expected_value, jobscnt)) {
//              expected_value = stats_maxjobscnt;
//      }
// // .. Will end up with a racy code instead :(
	if (jobscnt > stats_maxjobscnt) {
		// A race is possible here, but it won't lead to any serious consequences, in a worst
		// (and unlikely) case stats_maxjobscnt will be slightly lower than it actually should be
		stats_maxjobscnt = jobscnt;
	}

	auto eptr = csservEntries.begin();
	while (eptr != csservEntries.end()) {
		if (eptr->state == CLOSED) {
			tcpclose(eptr->sock);
			if (eptr->rpacket) {
				worker_delete_packet(eptr->rpacket);
			}
			if (eptr->wpacket) {
				worker_delete_preserved(eptr->wpacket);
			}
			if (eptr->fwdsock >= 0) {
				tcpclose(eptr->fwdsock);
			}
			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			if (eptr->fwdinputpacket.packet) {
				free(eptr->fwdinputpacket.packet);
			}
			packetstruct *pptr, *paptr;
			pptr = eptr->outputhead;
			while (pptr) {
				if (pptr->packet) {
					free(pptr->packet);
				}
				paptr = pptr;
				pptr = pptr->next;
				delete paptr;
			}
			eptr = csservEntries.erase(eptr);
		} else {
			++eptr;
		}
	}
}

void NetworkWorkerThread::askForTermination() {
	TRACETHIS();
	doTerminate = true;
}

void NetworkWorkerThread::addConnection(int newSocketFD) {
	TRACETHIS();
	tcpnonblock(newSocketFD);
	tcpnodelay(newSocketFD);

	std::unique_lock<std::mutex> lock(csservheadLock);
	csservEntries.emplace_front(newSocketFD, bgJobPool_);
	csservEntries.front().activity = eventloop_time();

	eassert(write(notify_pipe[1], "9", 1) == 1);
}
