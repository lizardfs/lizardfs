/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
*/

#include "config.h"

#define BGJOBSCNT 1000
#define NR_OF_BGJOBS_WORKERS 100

#include <time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <memory>
#include <set>

#include "devtools/TracePrinter.h"
#include "chunkserver/csserv.h"
#include "chunkserver/hddspacemgr.h"
#include "chunkserver/bgjobs.h"
#include "common/cfg.h"
#include "common/charts.h"
#include "common/cltocs_communication.h"
#include "common/cstocl_communication.h"
#include "common/cstocs_communication.h"
#include "common/datapack.h"
#include "common/massert.h"
#include "common/main.h"
#include "common/MFSCommunication.h"
#include "common/packet.h"
#include "common/slogger.h"
#include "common/sockets.h"

// connection timeout in seconds
#define CSSERV_TIMEOUT 30

#define CONNECT_RETRIES 10
#define CONNECT_TIMEOUT(cnt) (((cnt)%2)?(300000*(1<<((cnt)>>1))):(200000*(1<<((cnt)>>1))))

#define MaxPacketSize 100000

class MessageSerializer {
public:
	virtual void serializePrefixOfCstoclReadData(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint32_t offset, uint32_t size) = 0;
	virtual void serializeCstoclReadStatus(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint8_t status) = 0;
	virtual void serializeCltocsWriteInit(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType,
			const std::vector<NetworkAddress>& chain) = 0;
	virtual void serializeCstoclWriteStatus(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint32_t writeId, uint8_t status) = 0;
	virtual ~MessageSerializer() {}
};

class MooseFsMessageSerializer : public MessageSerializer {
public:
	static MooseFsMessageSerializer* getSingleton() {
		static MooseFsMessageSerializer singleton;
		return &singleton;
	}

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

	void serializeCltocsWriteInit(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType,
			const std::vector<NetworkAddress>& chain) {
		sassert(chunkType == ChunkType::getStandardChunkType());
		serializeMooseFsPacket(buffer, CLTOCS_WRITE, chunkId, chunkVersion, chain);
	}

	void serializeCstoclWriteStatus(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint32_t writeId, uint8_t status) {
		serializeMooseFsPacket(buffer, CSTOCL_WRITE_STATUS, chunkId, writeId, status);
	}
};


class LizardFsMessageSerializer : public MessageSerializer {
public:
	static LizardFsMessageSerializer* getSingleton() {
		static LizardFsMessageSerializer singleton;
		return &singleton;
	}

	void serializePrefixOfCstoclReadData(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint32_t offset, uint32_t size) {
		cstocl::readData::serializePrefix(buffer, chunkId, offset, size);
	}

	void serializeCstoclReadStatus(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint8_t status) {
		cstocl::readStatus::serialize(buffer, chunkId, status);
	}

	void serializeCltocsWriteInit(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint32_t chunkVersion, ChunkType chunkType,
			const std::vector<NetworkAddress>& chain) {
		cltocs::writeInit::serialize(buffer, chunkId, chunkVersion, chunkType, chain);
	}

	void serializeCstoclWriteStatus(std::vector<uint8_t>& buffer,
			uint64_t chunkId, uint32_t writeId, uint8_t status) {
		cstocl::writeStatus::serialize(buffer, chunkId, writeId, status);
	}
};

//csserventry.mode
enum ChunkserverEntryMode {
	HEADER, DATA
};

//csserventry.state
enum ChunkserverEntryState {
	IDLE,        // idle connection, new or used previously
	READ,        // after CLTOCS_READ, but didn't send all of the CSTOCL_READ_(DATA|STAUS)
	WRITELAST,   // connection ready for writing data; data is not forwarded to other chunkservers
	CONNECTING,  // we are now connecting to other chunkserver to form a writing chain
	WRITEINIT,   // we are sending packet forming a chain to the next chunkserver
	WRITEFWD,    // connection ready for writing data; data will be forwarded to other chunkservers
	WRITEFINISH, // write error occurred, will be closed after sending error status
	CLOSE,       // close request, it will immediately be changed to CLOSEWAIT or CLOSED
	CLOSEWAIT,   // waits for a worker to finish requested job, then will be closed
	CLOSED       // ready to be deleted
};

struct packetstruct {
	packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t *packet;
	std::unique_ptr<OutputBuffer> outputBuffer;

	packetstruct() : next(nullptr), startptr(nullptr), bytesleft(0), packet(nullptr) {
	}
};

struct csserventry {
	uint8_t state;
	uint8_t mode;
	uint8_t fwdmode;

	int sock;
	int fwdsock; // forwarding socket for writing
	uint64_t connstart; // 'connect' start time in usec (for timeout and retry)
	uint8_t connretrycnt; // 'connect' retry counter
	NetworkAddress fwdServer; // the next server in write chain
	int32_t pdescpos;
	int32_t fwdpdescpos;
	uint32_t activity;
	uint8_t hdrbuff[PacketHeader::kSize];
	uint8_t fwdhdrbuff[PacketHeader::kSize];
	packetstruct inputpacket;
	uint8_t *fwdstartptr; // used for forwarding inputpacket data
	uint32_t fwdbytesleft; // used for forwarding inputpacket data
	packetstruct fwdinputpacket; // used for receiving status from fwdsocket
	std::vector<uint8_t> fwdinitpacket; // used only for write initialization
	packetstruct *outputhead, **outputtail;

	/* write */
	uint32_t wjobid;
	uint32_t wjobwriteid;
	std::set<uint32_t> partiallyCompletedWrites; // writeId's which:
	// * have been completed by our worker, but need ack from the next chunkserver from the chain
	// * have been acked by the next chunkserver from the chain, but are still being written by us

	/* read */
	uint32_t rjobid;
	uint8_t todocnt; // R (read finished + send finished)

	/* get blocks */
	uint32_t getBlocksJobId;
	uint16_t getBlocksJobResult;

	/* common for read and write but meaning is different !!! */
	void *rpacket;
	void *wpacket;

	uint8_t chunkisopen;
	uint64_t chunkid; // R+W
	uint32_t version; // R+W
	ChunkType chunkType; // R
	uint32_t offset; // R
	uint32_t size; // R
	MessageSerializer* messageSerializer; // R+W

	struct csserventry *next;

	csserventry(int socket)
			: state(IDLE),
			  mode(HEADER),
			  fwdmode(HEADER),
			  sock(socket),
			  fwdsock(-1),
			  connstart(0),
			  connretrycnt(0),
			  pdescpos(-1),
			  fwdpdescpos(-1),
			  activity(0),
			  fwdstartptr(NULL),
			  fwdbytesleft(0),
			  outputhead(nullptr),
			  outputtail(&outputhead),
			  wjobid(0),
			  wjobwriteid(0),
			  rjobid(0),
			  todocnt(0),
			  getBlocksJobId(0),
			  getBlocksJobResult(0),
			  rpacket(nullptr),
			  wpacket(nullptr),
			  chunkisopen(0),
			  chunkid(0),
			  version(0),
			  chunkType(ChunkType::getStandardChunkType()),
			  offset(0),
			  size(0),
			  messageSerializer(nullptr),
			  next(nullptr) {
		inputpacket.bytesleft = 8;
		inputpacket.startptr = hdrbuff;
		inputpacket.packet = NULL;
	}

	csserventry(const csserventry&) = delete;
	csserventry& operator=(const csserventry&) = delete;
};

static csserventry *csservhead = NULL;
static int lsock;
static int32_t lsockpdescpos;

static void *jpool;
static int jobfd;
static int32_t jobfdpdescpos;

static uint32_t mylistenip;
static uint16_t mylistenport;

static uint64_t stats_bytesin = 0;
static uint64_t stats_bytesout = 0;
static uint32_t stats_hlopr = 0;
static uint32_t stats_hlopw = 0;
static uint32_t stats_maxjobscnt = 0;

// from config
static char *ListenHost;
static char *ListenPort;

void csserv_stats(uint64_t *bin, uint64_t *bout, uint32_t *hlopr,
		uint32_t *hlopw, uint32_t *maxjobscnt) {
	TRACETHIS();
	*bin = stats_bytesin;
	*bout = stats_bytesout;
	*hlopr = stats_hlopr;
	*hlopw = stats_hlopw;
	*maxjobscnt = stats_maxjobscnt;
	stats_bytesin = 0;
	stats_bytesout = 0;
	stats_hlopr = 0;
	stats_hlopw = 0;
	stats_maxjobscnt = 0;
}

packetstruct* csserv_create_detached_packet_with_output_buffer(
		const std::vector<uint8_t>& packetPrefix) {
	TRACETHIS();
	PacketHeader header;
	deserializePacketHeader(packetPrefix, header);
	uint32_t sizeOfWholePacket = PacketHeader::kSize + header.length;
	packetstruct* outPacket = new packetstruct();
	passert(outPacket);
#ifdef HAVE_SPLICE
	if (sizeOfWholePacket < 512 * 1024u) {
		outPacket->outputBuffer.reset(new AvoidingCopyingOutputBuffer(512 * 1024u));
	} else {
		outPacket->outputBuffer.reset(new SimpleOutputBuffer(sizeOfWholePacket));
	}
#else /* HAVE_SPLICE */
	outPacket->outputBuffer.reset(SimpleOutputBuffer(sizeOfWholePacket));
#endif /* HAVE_SPLICE */
	if (outPacket->outputBuffer->copyIntoBuffer(packetPrefix) != (ssize_t)packetPrefix.size()) {
		delete outPacket;
		return nullptr;
	}
	return outPacket;
}

uint8_t* csserv_get_packet_data(void *packet) {
	TRACETHIS();
	packetstruct *outpacket = (packetstruct*) packet;
	return outpacket->packet + 8;
}

void csserv_delete_packet(void *packet) {
  TRACETHIS();
  packetstruct *outpacket = (packetstruct*) packet;
	free(outpacket->packet);
  delete outpacket;
}

void csserv_attach_packet(csserventry *eptr, void *packet) {
	packetstruct *outpacket = (packetstruct*)packet;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
}

void* csserv_preserve_inputpacket(csserventry *eptr) {
	TRACETHIS();
	void* ret;
	ret = eptr->inputpacket.packet;
	eptr->inputpacket.packet = NULL;
	return ret;
}

void csserv_delete_preserved(void *p) {
	TRACETHIS();
	if (p) {
		free(p);
		p = NULL;
	}
}

void csserv_create_attached_packet(csserventry *eptr, const std::vector<uint8_t>& packet) {
	TRACETHIS();
	packetstruct* outpacket = new packetstruct();
	passert(outpacket);
	outpacket->packet = (uint8_t*) malloc(packet.size());
	passert(outpacket->packet);
	memcpy(outpacket->packet, packet.data(), packet.size());
	outpacket->bytesleft = packet.size();
	outpacket->startptr = outpacket->packet;
	csserv_attach_packet(eptr, outpacket);
}

uint8_t* csserv_create_attached_packet(csserventry *eptr, uint32_t type, uint32_t size) {
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
	csserv_attach_packet(eptr, outpacket);
	return ptr;
}

void csserv_fwderror(csserventry *eptr) {
	TRACETHIS();
	sassert(eptr->messageSerializer != NULL);
	std::vector<uint8_t> buffer;
	uint8_t status = (eptr->state == CONNECTING ? ERROR_CANTCONNECT : ERROR_DISCONNECTED);
	eptr->messageSerializer->serializeCstoclWriteStatus(buffer, eptr->chunkid, 0, status);
	csserv_create_attached_packet(eptr, buffer);
	eptr->state = WRITEFINISH;
}

// initialize connection to another CS
int csserv_initconnect(csserventry *eptr) {
	TRACETHIS();
	int status;
	// TODO(msulikowski) If we want to use a ConnectionPool, this is the right place
	// to get a connection from it
	eptr->fwdsock = tcpsocket();
	if (eptr->fwdsock < 0) {
		mfs_errlog(LOG_WARNING, "create socket, error");
		return -1;
	}
	if (tcpnonblock(eptr->fwdsock) < 0) {
		mfs_errlog(LOG_WARNING, "set nonblock, error");
		tcpclose(eptr->fwdsock);
		eptr->fwdsock = -1;
		return -1;
	}
	status = tcpnumconnect(eptr->fwdsock, eptr->fwdServer.ip, eptr->fwdServer.port);
	if (status < 0) {
		mfs_errlog(LOG_WARNING, "connect failed, error");
		tcpclose(eptr->fwdsock);
		eptr->fwdsock = -1;
		return -1;
	}
	if (status == 0) { // connected immediately
		tcpnodelay(eptr->fwdsock);
		eptr->state = WRITEINIT;
	} else {
		eptr->state = CONNECTING;
		eptr->connstart = main_utime();
	}
	return 0;
}

void csserv_retryconnect(csserventry *eptr) {
	TRACETHIS();
	tcpclose(eptr->fwdsock);
	eptr->fwdsock = -1;
	eptr->connretrycnt++;
	if (eptr->connretrycnt < CONNECT_RETRIES) {
		if (csserv_initconnect(eptr) < 0) {
			csserv_fwderror(eptr);
			return;
		}
	} else {
		csserv_fwderror(eptr);
		return;
	}
}

void csserv_check_nextpacket(csserventry *eptr);

// common - delayed close
void csserv_delayed_close(uint8_t status, void *e) {
	TRACETHIS();
	csserventry *eptr = (csserventry*) e;
	if (eptr->wjobid > 0 && eptr->wjobwriteid == 0 && status == STATUS_OK) { // this was job_open
		eptr->chunkisopen = 1;
	}
	if (eptr->chunkisopen) {
		job_close(jpool, NULL, NULL, eptr->chunkid, eptr->chunkType);
		eptr->chunkisopen = 0;
	}
	eptr->state = CLOSED;
}

// bg reading

void csserv_read_continue(csserventry *eptr, bool isFirst = false);

void csserv_read_finished(uint8_t status, void *e) {
	TRACETHIS();
	csserventry *eptr = (csserventry*) e;
	eptr->rjobid = 0;
	if (status == STATUS_OK) {
		eptr->todocnt--;
		if (eptr->todocnt == 0) {
			csserv_read_continue(eptr);
		}
	} else {
		if (eptr->rpacket) {
			csserv_delete_packet(eptr->rpacket);
			eptr->rpacket = NULL;
		}
		std::vector<uint8_t> buffer;
		eptr->messageSerializer->serializeCstoclReadStatus(buffer, eptr->chunkid, status);
		csserv_create_attached_packet(eptr, buffer);
		job_close(jpool, NULL, NULL, eptr->chunkid, eptr->chunkType);
		eptr->chunkisopen = 0;
		eptr->state = IDLE; // after sending status even if there was an error it's possible to
		// receive new requests on the same connection
	}
}

void csserv_send_finished(csserventry *eptr) {
	TRACETHIS();
	eptr->todocnt--;
	if (eptr->todocnt == 0) {
		csserv_read_continue(eptr);
	}
}

void csserv_read_continue(csserventry *eptr, bool isFirst) {
	TRACETHIS2(eptr->offset, eptr->size);
	uint32_t size;

	if (eptr->rpacket) {
		csserv_attach_packet(eptr, eptr->rpacket);
		eptr->rpacket = NULL;
		eptr->todocnt++;
	}
	if (eptr->size == 0) { // everything has been read
		std::vector<uint8_t> buffer;
		eptr->messageSerializer->serializeCstoclReadStatus(buffer, eptr->chunkid, STATUS_OK);
		csserv_create_attached_packet(eptr, buffer);
		job_close(jpool, NULL, NULL, eptr->chunkid, eptr->chunkType);
		eptr->chunkisopen = 0;
		eptr->state = IDLE; // no error - do not disconnect - go direct to the IDLE state, ready for requests on the same connection
	} else {
			size = eptr->size;
		if (size > MFSBLOCKSIZE) {
			size = MFSBLOCKSIZE;
		}
		std::vector<uint8_t> readDataPrefix;
		eptr->messageSerializer->serializePrefixOfCstoclReadData(readDataPrefix,
				eptr->chunkid, eptr->offset, size);
		packetstruct* packet = csserv_create_detached_packet_with_output_buffer(readDataPrefix);
		if (packet == nullptr) {
			eptr->state = CLOSE;
			return;
		}
		eptr->rpacket = (void*)packet;
		eptr->rjobid = job_read(jpool, csserv_read_finished, eptr, eptr->chunkid,
				eptr->version, eptr->chunkType, eptr->offset, size,
				packet->outputBuffer.get(), isFirst);
		if (eptr->rjobid == 0) {
			eptr->state = CLOSE;
			return;
		}
		eptr->todocnt++;
		eptr->offset += size;
		eptr->size -= size;
	}
}

void csserv_ping(csserventry *eptr, const uint8_t *data, PacketHeader::Length length) {
	if (length != 4) {
		eptr->state = CLOSE;
		return;
	}

	uint32_t size;
	deserialize(data, length, size);
	csserv_create_attached_packet(eptr, ANTOAN_PING_REPLY, size);
}

void csserv_read_init(csserventry *eptr, const uint8_t *data,
		PacketHeader::Type type, PacketHeader::Length length) {
	TRACETHIS2(type, length);

	// Deserialize request
	sassert(type == LIZ_CLTOCS_READ || type == CLTOCS_READ);
	try {
		if (type == LIZ_CLTOCS_READ) {
			cltocs::read::deserialize(data, length,
					eptr->chunkid,
					eptr->version,
					eptr->chunkType,
					eptr->offset,
					eptr->size);
			eptr->messageSerializer = LizardFsMessageSerializer::getSingleton();
		} else {
			deserializeAllMooseFsPacketDataNoHeader(data, length,
					eptr->chunkid,
					eptr->version,
					eptr->offset,
					eptr->size);
			eptr->chunkType = ChunkType::getStandardChunkType();
			eptr->messageSerializer = MooseFsMessageSerializer::getSingleton();
		}
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
				eptr->chunkid, STATUS_OK);
	} else if (eptr->size > MFSCHUNKSIZE) {
		eptr->messageSerializer->serializeCstoclReadStatus(instantResponseBuffer,
				eptr->chunkid, ERROR_WRONGSIZE);
	} else if (eptr->offset >= MFSCHUNKSIZE || eptr->offset + eptr->size > MFSCHUNKSIZE) {
		eptr->messageSerializer->serializeCstoclReadStatus(instantResponseBuffer,
				eptr->chunkid, ERROR_WRONGOFFSET);
	}
	if (!instantResponseBuffer.empty()) {
		csserv_create_attached_packet(eptr, instantResponseBuffer);
		return;
	}
	// Process the request
	stats_hlopr++;
	eptr->chunkisopen = 1;
	eptr->state = READ;
	eptr->todocnt = 0;
	eptr->rjobid = 0;
	csserv_read_continue(eptr, true);
}

// bg writing

void csserv_write_finished(uint8_t status, void *e) {
	TRACETHIS();
	csserventry *eptr = (csserventry*) e;
	eptr->wjobid = 0;
	sassert(eptr->messageSerializer != NULL);
	if (status != STATUS_OK) {
		std::vector<uint8_t> buffer;
		eptr->messageSerializer->serializeCstoclWriteStatus(buffer,
				eptr->chunkid, eptr->wjobwriteid, status);
		csserv_create_attached_packet(eptr, buffer);
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
		csserv_create_attached_packet(eptr, buffer);
	} else {
		if (eptr->partiallyCompletedWrites.count(eptr->wjobwriteid) > 0) {
			// found - it means that it was added by status_receive, ie. next chunkserver from
			// a chain finished writing before our worker
			sassert(eptr->messageSerializer != NULL);
			std::vector<uint8_t> buffer;
			eptr->messageSerializer->serializeCstoclWriteStatus(buffer,
					eptr->chunkid, eptr->wjobwriteid, STATUS_OK);
			csserv_create_attached_packet(eptr, buffer);
			eptr->partiallyCompletedWrites.erase(eptr->wjobwriteid);
		} else {
			// not found - so add it
			eptr->partiallyCompletedWrites.insert(eptr->wjobwriteid);
		}
	}
	csserv_check_nextpacket(eptr);
}

void csserv_write_init(csserventry *eptr,
		const uint8_t *data, PacketHeader::Type type, PacketHeader::Length length) {
	TRACETHIS();
	std::vector<NetworkAddress> chain;

	sassert(type == LIZ_CLTOCS_WRITE_INIT || type == CLTOCS_WRITE);
	try {
		if (type == LIZ_CLTOCS_WRITE_INIT) {
			cltocs::writeInit::deserialize(data, length,
					eptr->chunkid, eptr->version, eptr->chunkType, chain);
			eptr->messageSerializer = LizardFsMessageSerializer::getSingleton();
		} else {
			deserializeAllMooseFsPacketDataNoHeader(data, length,
				eptr->chunkid, eptr->version, chain);
			eptr->chunkType = ChunkType::getStandardChunkType();
			eptr->messageSerializer = MooseFsMessageSerializer::getSingleton();
		}
	} catch (IncorrectDeserializationException& ex) {
		syslog(LOG_NOTICE, "Received malformed WRITE_INIT message (length: %" PRIu32 ")", length);
		eptr->state = CLOSE;
		return;
	}

	if (!chain.empty()) {
		// Create a chain -- connect to the next chunkserver
		eptr->fwdServer = chain[0];
		chain.erase(chain.begin());
		eptr->messageSerializer->serializeCltocsWriteInit(eptr->fwdinitpacket,
				eptr->chunkid, eptr->version, eptr->chunkType, chain);
		eptr->fwdstartptr = eptr->fwdinitpacket.data();
		eptr->fwdbytesleft = eptr->fwdinitpacket.size();
		eptr->connretrycnt = 0;
		if (csserv_initconnect(eptr) < 0) {
			std::vector<uint8_t> buffer;
			eptr->messageSerializer->serializeCstoclWriteStatus(buffer,
					eptr->chunkid, 0, ERROR_CANTCONNECT);
			csserv_create_attached_packet(eptr, buffer);
			eptr->state = WRITEFINISH;
			return;
		}
	} else {
		eptr->state = WRITELAST;
	}
	stats_hlopw++;
	eptr->wjobwriteid = 0;
	eptr->wjobid = job_open(jpool, csserv_write_finished, eptr, eptr->chunkid, eptr->chunkType);
}

void csserv_write_data(csserventry *eptr,
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
		if (type == LIZ_CLTOCS_WRITE_DATA) {
			if (eptr->messageSerializer != LizardFsMessageSerializer::getSingleton()) {
				syslog(LOG_NOTICE, "Received WRITE_DATA message incompatible with WRITE_INIT");
				eptr->state = CLOSE;
				return;
			}
			cltocs::writeData::deserializePrefix(data, length,
					chunkId, writeId, blocknum, offset, size, crc);
			dataToWrite = data + cltocs::writeData::kPrefixSize;
		} else {
			if (eptr->messageSerializer != MooseFsMessageSerializer::getSingleton()) {
				syslog(LOG_NOTICE, "Received WRITE_DATA message incompatible with WRITE_INIT");
				eptr->state = CLOSE;
				return;
			}
			uint16_t offset16;
			deserializeAllMooseFsPacketDataNoHeader(data, length,
				chunkId, writeId, blocknum, offset16, size, crc, dataToWrite);
			offset = offset16;
			sassert(eptr->chunkType == ChunkType::getStandardChunkType());
		}
	} catch (IncorrectDeserializationException&) {
		syslog(LOG_NOTICE, "Received malformed WRITE_DATA message (length: %" PRIu32 ")", length);
		eptr->state = CLOSE;
		return;
	}

	uint8_t status = STATUS_OK;
	uint32_t dataSize = data + length - dataToWrite;
	if (dataSize != size) {
		status = ERROR_WRONGSIZE;
	} else if (chunkId != eptr->chunkid) {
		status = ERROR_WRONGCHUNKID;
	}

	if (status != STATUS_OK) {
		std::vector<uint8_t> buffer;
		eptr->messageSerializer->serializeCstoclWriteStatus(buffer, chunkId, writeId, status);
		csserv_create_attached_packet(eptr, buffer);
		eptr->state = WRITEFINISH;
		return;
	}
	if (eptr->wpacket) {
		csserv_delete_preserved(eptr->wpacket);
	}
	eptr->wpacket = csserv_preserve_inputpacket(eptr);
	eptr->wjobwriteid = writeId;
	eptr->wjobid = job_write(jpool, csserv_write_finished, eptr,
			chunkId, eptr->version, eptr->chunkType,
			blocknum, offset, size, crc, dataToWrite);
}

void csserv_write_status(csserventry *eptr,
		const uint8_t *data, PacketHeader::Type type, PacketHeader::Length length) {
	TRACETHIS();
	uint64_t chunkId;
	uint32_t writeId;
	uint8_t status;

	sassert(type == LIZ_CSTOCL_WRITE_STATUS || type == CSTOCL_WRITE_STATUS);
	sassert(eptr->messageSerializer != NULL);
	try {
		if (type == LIZ_CSTOCL_WRITE_STATUS) {
			if (eptr->messageSerializer != LizardFsMessageSerializer::getSingleton()) {
				syslog(LOG_NOTICE, "Received WRITE_STATUS message incompatible with WRITE_INIT");
				eptr->state = CLOSE;
				return;
			}
			std::vector<uint8_t> message(data, data + length);
			cstocl::writeStatus::deserialize(message, chunkId, writeId, status);
		} else {
			if (eptr->messageSerializer != MooseFsMessageSerializer::getSingleton()) {
				syslog(LOG_NOTICE, "Received WRITE_STATUS message incompatible with the WRITE_INIT");
				eptr->state = CLOSE;
				return;
			}
			deserializeAllMooseFsPacketDataNoHeader(data, length, chunkId, writeId, status);
			sassert(eptr->chunkType == ChunkType::getStandardChunkType());
		}
	} catch (IncorrectDeserializationException&) {
		syslog(LOG_NOTICE, "Received malformed WRITE_STATUS message (length: %" PRIu32 ")", length);
		eptr->state = CLOSE;
		return;
	}

	if (eptr->chunkid != chunkId) {
		status = ERROR_WRONGCHUNKID;
		writeId = 0;
	}

	if (status != STATUS_OK) {
		std::vector<uint8_t> buffer;
		eptr->messageSerializer->serializeCstoclWriteStatus(buffer, chunkId, writeId, status);
		csserv_create_attached_packet(eptr, buffer);
		eptr->state = WRITEFINISH;
		return;
	}

	if (eptr->partiallyCompletedWrites.count(writeId) > 0) {
		// found - means it was added by write_finished
		std::vector<uint8_t> buffer;
		eptr->messageSerializer->serializeCstoclWriteStatus(buffer, chunkId, writeId, STATUS_OK);
		csserv_create_attached_packet(eptr, buffer);
		eptr->partiallyCompletedWrites.erase(writeId);
	} else {
		// if not found then add record
		eptr->partiallyCompletedWrites.insert(writeId);
	}
}

void csserv_write_end(csserventry *eptr, const uint8_t* data, uint32_t length) {
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
		job_close(jpool, NULL, NULL, eptr->chunkid, eptr->chunkType);
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

void csserv_liz_get_chunk_blocks_finished(uint8_t status, void *extra) {
	TRACETHIS();
	csserventry *eptr = (csserventry*) extra;
	eptr->getBlocksJobId = 0;
	std::vector<uint8_t> buffer;
	cstocs::getChunkBlocksStatus::serialize(buffer, eptr->chunkid, eptr->version, eptr->chunkType,
			eptr->getBlocksJobResult, status);
	csserv_create_attached_packet(eptr, buffer);
}

void csserv_get_chunk_blocks_finished(uint8_t status, void *extra) {
	TRACETHIS();
	csserventry *eptr = (csserventry*) extra;
	eptr->getBlocksJobId = 0;
	std::vector<uint8_t> buffer;
	serializeMooseFsPacket(buffer, CSTOCS_GET_CHUNK_BLOCKS_STATUS,
			eptr->chunkid, eptr->version, eptr->getBlocksJobResult, status);
	csserv_create_attached_packet(eptr, buffer);
}

void csserv_liz_get_chunk_blocks(csserventry *eptr, const uint8_t *data, uint32_t length) {
	cstocs::getChunkBlocks::deserialize(data, length,
			eptr->chunkid, eptr->version, eptr->chunkType);
	eptr->getBlocksJobId = job_get_blocks(jpool, csserv_liz_get_chunk_blocks_finished, eptr,
			eptr->chunkid, eptr->version, eptr->chunkType, &(eptr->getBlocksJobResult));
}

void csserv_get_chunk_blocks(csserventry *eptr, const uint8_t *data,
		uint32_t length) {
	deserializeAllMooseFsPacketDataNoHeader(data, length, eptr->chunkid, eptr->version);
	eptr->chunkType = ChunkType::getStandardChunkType();
	eptr->getBlocksJobId = job_get_blocks(jpool, csserv_get_chunk_blocks_finished, eptr,
			eptr->chunkid, eptr->version, eptr->chunkType, &(eptr->getBlocksJobResult));
}

/* IDLE operations */

void csserv_hdd_list_v1(csserventry *eptr, const uint8_t *data,
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
	ptr = csserv_create_attached_packet(eptr, CSTOCL_HDD_LIST_V1, l);
	hdd_diskinfo_v1_data(ptr); // unlock
}

void csserv_hdd_list_v2(csserventry *eptr, const uint8_t *data,
		uint32_t length) {
	TRACETHIS();
	uint32_t l;
	uint8_t *ptr;

	(void) data;
	if (length != 0) {
		syslog(LOG_NOTICE,"CLTOCS_HDD_LIST(2) - wrong size (%" PRIu32 "/0)",length);
		eptr->state = CLOSE;
		return;
	}
	l = hdd_diskinfo_v2_size(); // lock
	ptr = csserv_create_attached_packet(eptr, CSTOCL_HDD_LIST_V2, l);
	hdd_diskinfo_v2_data(ptr); // unlock
}

void csserv_chart(csserventry *eptr, const uint8_t *data, uint32_t length) {
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
		ptr = csserv_create_attached_packet(eptr, ANTOCL_CHART, l);
		if (l > 0) {
			charts_get_png(ptr);
		}
	} else {
		l = charts_make_csv(chartid % CHARTS_CSV_CHARTID_BASE);
		ptr = csserv_create_attached_packet(eptr,ANTOCL_CHART,l);
		if (l>0) {
			charts_get_csv(ptr);
		}
	}
}

void csserv_chart_data(csserventry *eptr, const uint8_t *data, uint32_t length) {
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
	ptr = csserv_create_attached_packet(eptr, ANTOCL_CHART_DATA, l);
	if (l > 0) {
		charts_makedata(ptr, chartid);
	}
}


void csserv_outputcheck(csserventry *eptr) {
	TRACETHIS();
	if (eptr->state == READ) {
		csserv_send_finished(eptr);
	}
}

void csserv_close(csserventry *eptr) {
	TRACETHIS();
	if (eptr->rjobid > 0) {
		job_pool_disable_job(jpool, eptr->rjobid);
		job_pool_change_callback(jpool, eptr->rjobid, csserv_delayed_close, eptr);
		eptr->state = CLOSEWAIT;
	} else if (eptr->wjobid > 0) {
		job_pool_disable_job(jpool, eptr->wjobid);
		job_pool_change_callback(jpool, eptr->wjobid, csserv_delayed_close, eptr);
		eptr->state = CLOSEWAIT;
	} else if (eptr->getBlocksJobId > 0) {
		job_pool_disable_job(jpool, eptr->getBlocksJobId);
		job_pool_change_callback(jpool, eptr->getBlocksJobId, csserv_delayed_close, eptr);
		eptr->state = CLOSEWAIT;
	} else {
		if (eptr->chunkisopen) {
			job_close(jpool, NULL, NULL, eptr->chunkid, eptr->chunkType);
			eptr->chunkisopen = 0;
		}
		eptr->state = CLOSED;
	}
}

void csserv_gotpacket(csserventry *eptr, uint32_t type, const uint8_t *data, uint32_t length) {
	TRACETHIS();
//	syslog(LOG_NOTICE,"packet %u:%u",type,length);
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
			csserv_ping(eptr, data, length);
			break;
		case CLTOCS_READ:
		case LIZ_CLTOCS_READ:
			csserv_read_init(eptr, data, type, length);
			break;
		case CLTOCS_WRITE:
		case LIZ_CLTOCS_WRITE_INIT:
			csserv_write_init(eptr, data, type, length);
			break;
		case CSTOCS_GET_CHUNK_BLOCKS:
			csserv_get_chunk_blocks(eptr, data, length);
			break;
		case LIZ_CSTOCS_GET_CHUNK_BLOCKS:
			csserv_liz_get_chunk_blocks(eptr, data, length);
			break;
		case CLTOCS_HDD_LIST_V1:
			csserv_hdd_list_v1(eptr, data, length);
			break;
		case CLTOCS_HDD_LIST_V2:
			csserv_hdd_list_v2(eptr, data, length);
			break;
		case CLTOAN_CHART:
			csserv_chart(eptr, data, length);
			break;
		case CLTOAN_CHART_DATA:
			csserv_chart_data(eptr, data, length);
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
			csserv_write_data(eptr, data, type, length);
			break;
		case LIZ_CLTOCS_WRITE_END:
			csserv_write_end(eptr, data, length);
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
			csserv_write_data(eptr, data, type, length);
			break;
		case CSTOCL_WRITE_STATUS:
		case LIZ_CSTOCL_WRITE_STATUS:
			csserv_write_status(eptr, data, type, length);
			break;
		case LIZ_CLTOCS_WRITE_END:
			csserv_write_end(eptr, data, length);
			break;
		default:
			syslog(LOG_NOTICE, "Got invalid message in WRITEFWD state (type:%" PRIu32 ")",type);
			eptr->state = CLOSE;
			break;
		}
	} else if (eptr->state == WRITEFINISH) {
		if (type == CLTOCS_WRITE_DATA
				|| type == LIZ_CLTOCS_WRITE_DATA
				|| type == LIZ_CLTOCS_WRITE_END) {
			return;
		} else {
			syslog(LOG_NOTICE, "Got invalid message in WRITEFINISH state (type:%" PRIu32 ")",type);
			eptr->state = CLOSE;
		}
	} else {
		syslog(LOG_NOTICE, "Got invalid message (type:%" PRIu32 ")",type);
		eptr->state = CLOSE;
	}
}

void csserv_term(void) {
	TRACETHIS();
	csserventry *eptr, *eaptr;
	packetstruct *pptr, *paptr;

	syslog(LOG_NOTICE, "closing %s:%s", ListenHost, ListenPort);
	tcpclose(lsock);

	job_pool_delete(jpool);

	eptr = csservhead;
	while (eptr) {
		if (eptr->chunkisopen) {
			hdd_close(eptr->chunkid, eptr->chunkType);
		}
		tcpclose(eptr->sock);
		if (eptr->fwdsock >= 0) {
			tcpclose(eptr->fwdsock);
		}
		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		if (eptr->wpacket) {
			csserv_delete_preserved(eptr->wpacket);
		}
		if (eptr->fwdinputpacket.packet) {
			free(eptr->fwdinputpacket.packet);
		}
		pptr = eptr->outputhead;
		while (pptr) {
			if (pptr->packet) {
				free(pptr->packet);
			}
			paptr = pptr;
			pptr = pptr->next;
			delete paptr;
		}
		eaptr = eptr;
		eptr = eptr->next;
		delete eaptr;
	}
	csservhead = NULL;
	free(ListenHost);
	free(ListenPort);
}

void csserv_check_nextpacket(csserventry *eptr) {
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

			csserv_gotpacket(eptr, type, eptr->inputpacket.packet + 8, size);

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

			csserv_gotpacket(eptr, type, eptr->inputpacket.packet, size);

			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet = NULL;
		}
	}
}

void csserv_fwdconnected(csserventry *eptr) {
	TRACETHIS();
	int status;
	status = tcpgetstatus(eptr->fwdsock);
	if (status) {
		mfs_errlog_silent(LOG_WARNING, "connection failed, error");
		csserv_fwderror(eptr);
		return;
	}
	tcpnodelay(eptr->fwdsock);
	eptr->state = WRITEINIT;
}

void csserv_fwdread(csserventry *eptr) {
	TRACETHIS();
	int32_t i;
	uint32_t type, size;
	const uint8_t *ptr;
	if (eptr->fwdmode == HEADER) {
		i = read(eptr->fwdsock, eptr->fwdinputpacket.startptr,
				eptr->fwdinputpacket.bytesleft);
		if (i == 0) {
//			syslog(LOG_NOTICE,"(fwdread) connection closed");
			csserv_fwderror(eptr);
			return;
		}
		if (i < 0) {
			if (errno != EAGAIN) {
				mfs_errlog_silent(LOG_NOTICE, "(fwdread) read error");
				csserv_fwderror(eptr);
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
			csserv_fwderror(eptr);
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
//				syslog(LOG_NOTICE,"(fwdread) connection closed");
				csserv_fwderror(eptr);
				return;
			}
			if (i < 0) {
				if (errno != EAGAIN) {
					mfs_errlog_silent(LOG_NOTICE, "(fwdread) read error");
					csserv_fwderror(eptr);
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

		csserv_gotpacket(eptr, type, eptr->fwdinputpacket.packet, size);

		if (eptr->fwdinputpacket.packet) {
			free(eptr->fwdinputpacket.packet);
		}
		eptr->fwdinputpacket.packet = NULL;
	}
}

void csserv_fwdwrite(csserventry *eptr) {
	TRACETHIS();
	int32_t i;
	if (eptr->fwdbytesleft > 0) {
		i = write(eptr->fwdsock, eptr->fwdstartptr, eptr->fwdbytesleft);
		if (i == 0) {
//			syslog(LOG_NOTICE,"(fwdwrite) connection closed");
			csserv_fwderror(eptr);
			return;
		}
		if (i < 0) {
			if (errno != EAGAIN) {
				mfs_errlog_silent(LOG_NOTICE, "(fwdwrite) write error");
				csserv_fwderror(eptr);
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

void csserv_forward(csserventry *eptr) {
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
				mfs_errlog_silent(LOG_NOTICE, "(forward) read error");
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
				mfs_errlog_silent(LOG_NOTICE, "(forward) read error: %s");
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
			csserv_fwderror(eptr);
			return;
		}
		if (i < 0) {
			if (errno != EAGAIN) {
				mfs_errlog_silent(LOG_NOTICE, "(forward) write error: %s");
				csserv_fwderror(eptr);
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
		csserv_gotpacket(eptr, header.type, packetData, header.length);
		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		eptr->inputpacket.packet = NULL;
		eptr->fwdstartptr = NULL;
	}
}

void csserv_read(csserventry *eptr) {
	TRACETHIS();
	int32_t i;
	uint32_t type, size;
	const uint8_t *ptr;

	if (eptr->mode == HEADER) {
		i = read(eptr->sock, eptr->inputpacket.startptr,
				eptr->inputpacket.bytesleft);
		if (i == 0) {
//			syslog(LOG_NOTICE,"(read) connection closed");
			eptr->state = CLOSE;
			return;
		}
		if (i < 0) {
			if (errno != EAGAIN) {
				mfs_errlog_silent(LOG_NOTICE, "(read) read error");
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
//				syslog(LOG_NOTICE,"(read) connection closed");
				eptr->state = CLOSE;
				return;
			}
			if (i < 0) {
				if (errno != EAGAIN) {
					mfs_errlog_silent(LOG_NOTICE, "(read) read error");
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

				csserv_gotpacket(eptr, type, eptr->inputpacket.packet, size);

			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet = NULL;
		}
	}
}

void csserv_write(csserventry *eptr) {
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
				mfs_errlog_silent(LOG_NOTICE, "(write) write error");
				eptr->state = CLOSE;
				return;
			} else if (ret == OutputBuffer::WRITE_AGAIN) {
				return;
			}
		} else {
			i = write(eptr->sock, pack->startptr, pack->bytesleft);
			if (i == 0) {
//			syslog(LOG_NOTICE,"(write) connection closed");
			eptr->state = CLOSE;
			return;
		}
			if (i < 0) {
				if (errno != EAGAIN) {
					mfs_errlog_silent(LOG_NOTICE, "(write) write error");
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
		csserv_outputcheck(eptr);
	}
}

void csserv_desc(struct pollfd *pdesc, uint32_t *ndesc) {
	TRACETHIS();
	uint32_t pos = *ndesc;
	csserventry *eptr;
	pdesc[pos].fd = lsock;
	pdesc[pos].events = POLLIN;
	lsockpdescpos = pos;
	pos++;
	pdesc[pos].fd = jobfd;
	pdesc[pos].events = POLLIN;
	jobfdpdescpos = pos;
	pos++;

	for (eptr = csservhead; eptr; eptr = eptr->next) {
		eptr->pdescpos = -1;
		eptr->fwdpdescpos = -1;
		switch (eptr->state) {
			case IDLE:
			case READ:
			case WRITELAST:
				pdesc[pos].fd = eptr->sock;
				pdesc[pos].events = 0;
				eptr->pdescpos = pos;
				if (eptr->inputpacket.bytesleft > 0) {
					pdesc[pos].events |= POLLIN;
				}
				if (eptr->outputhead != NULL) {
					pdesc[pos].events |= POLLOUT;
				}
				pos++;
				break;
			case CONNECTING:
				pdesc[pos].fd = eptr->fwdsock;
				pdesc[pos].events = POLLOUT;
				eptr->fwdpdescpos = pos;
				pos++;
				break;
			case WRITEINIT:
				if (eptr->fwdbytesleft > 0) {
					pdesc[pos].fd = eptr->fwdsock;
					pdesc[pos].events = POLLOUT;
					eptr->fwdpdescpos = pos;
					pos++;
				}
				break;
			case WRITEFWD:
				pdesc[pos].fd = eptr->fwdsock;
				pdesc[pos].events = POLLIN;
				eptr->fwdpdescpos = pos;
				if (eptr->fwdbytesleft > 0) {
					pdesc[pos].events |= POLLOUT;
				}
				pos++;

				pdesc[pos].fd = eptr->sock;
				pdesc[pos].events = 0;
				eptr->pdescpos = pos;
				if (eptr->inputpacket.bytesleft > 0) {
					pdesc[pos].events |= POLLIN;
				}
				if (eptr->outputhead != NULL) {
					pdesc[pos].events |= POLLOUT;
				}
				pos++;
				break;
			case WRITEFINISH:
				if (eptr->outputhead != NULL) {
					pdesc[pos].fd = eptr->sock;
					pdesc[pos].events = POLLOUT;
					eptr->pdescpos = pos;
					pos++;
				}
				break;
		}
	}
	*ndesc = pos;
}

void csserv_serve(struct pollfd *pdesc) {
	TRACETHIS();
	uint32_t now = main_time();
	uint64_t usecnow = main_utime();
	csserventry *eptr, **kptr;
	packetstruct *pptr, *paptr;
	uint32_t jobscnt;
	int ns;
	uint8_t lstate;

	if (lsockpdescpos >= 0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
		ns = tcpaccept(lsock);
		if (ns < 0) {
			mfs_errlog_silent(LOG_NOTICE, "accept error");
		} else {
			if (job_pool_jobs_count(jpool) >= (BGJOBSCNT * 9) / 10) {
				syslog(LOG_WARNING, "jobs queue is full !!!");
				tcpclose(ns);
			} else {
				tcpnonblock(ns);
				tcpnodelay(ns);
				eptr = new csserventry(ns);
				eptr->activity = now;
				eptr->next = csservhead;
				csservhead = eptr;
			}
		}
	}
	if (jobfdpdescpos >= 0 && (pdesc[jobfdpdescpos].revents & POLLIN)) {
		job_pool_check_jobs(jpool);
	}
	for (eptr = csservhead; eptr; eptr = eptr->next) {
		if (eptr->pdescpos >= 0
				&& (pdesc[eptr->pdescpos].revents & (POLLERR | POLLHUP))) {
			eptr->state = CLOSE;
		} else if (eptr->fwdpdescpos >= 0
				&& (pdesc[eptr->fwdpdescpos].revents & (POLLERR | POLLHUP))) {
			csserv_fwderror(eptr);
		}
		lstate = eptr->state;
		if (lstate == IDLE || lstate == READ || lstate == WRITELAST || lstate == WRITEFINISH) {
			if (eptr->pdescpos >= 0 && (pdesc[eptr->pdescpos].revents & POLLIN)) {
				eptr->activity = now;
				csserv_read(eptr);
			}
			if (eptr->pdescpos >= 0 && (pdesc[eptr->pdescpos].revents & POLLOUT)
					&& eptr->state == lstate) {
				eptr->activity = now;
				csserv_write(eptr);
			}
		} else if (lstate == CONNECTING && eptr->fwdpdescpos >= 0
				&& (pdesc[eptr->fwdpdescpos].revents & POLLOUT)) { // FD_ISSET(eptr->fwdsock,wset)) {
			eptr->activity = now;
			csserv_fwdconnected(eptr);
			if (eptr->state == WRITEINIT) {
				csserv_fwdwrite(eptr); // after connect likely some data can be send
			}
			if (eptr->state == WRITEFWD) {
				csserv_forward(eptr); // and also some data can be forwarded
			}
		} else if (eptr->state == WRITEINIT && eptr->fwdpdescpos >= 0
				&& (pdesc[eptr->fwdpdescpos].revents & POLLOUT)) { // FD_ISSET(eptr->fwdsock,wset)) {
			eptr->activity = now;
			csserv_fwdwrite(eptr); // after sending init packet
			if (eptr->state == WRITEFWD) {
				csserv_forward(eptr); // likely some data can be forwarded
			}
		} else if (eptr->state == WRITEFWD) {
			if ((eptr->pdescpos >= 0 && (pdesc[eptr->pdescpos].revents & POLLIN))
					|| (eptr->fwdpdescpos >= 0
							&& (pdesc[eptr->fwdpdescpos].revents & POLLOUT))) {
				eptr->activity = now;
				csserv_forward(eptr);
			}
			if (eptr->fwdpdescpos >= 0 && (pdesc[eptr->fwdpdescpos].revents & POLLIN)
					&& eptr->state == lstate) {
				eptr->activity = now;
				csserv_fwdread(eptr);
			}
			if (eptr->pdescpos >= 0 && (pdesc[eptr->pdescpos].revents & POLLOUT)
					&& eptr->state == lstate) {
				eptr->activity = now;
				csserv_write(eptr);
			}
		}
		if (eptr->state == WRITEFINISH && eptr->outputhead == NULL) {
			eptr->state = CLOSE;
		}
		if (eptr->state == CONNECTING
				&& eptr->connstart + CONNECT_TIMEOUT(eptr->connretrycnt) < usecnow) {
			csserv_retryconnect(eptr);
		}
		if (eptr->state != CLOSE && eptr->state != CLOSEWAIT
				&& eptr->state != CLOSED && eptr->activity + CSSERV_TIMEOUT < now) {
			// Close connection if inactive for more than CSSERV_TIMEOUT seconds
			eptr->state = CLOSE;
		}
		if (eptr->state == CLOSE) {
			csserv_close(eptr);
		}
	}
	jobscnt = job_pool_jobs_count(jpool);
	if (jobscnt >= stats_maxjobscnt) {
		stats_maxjobscnt = jobscnt;
	}
	kptr = &csservhead;
	while ((eptr = *kptr)) {
		if (eptr->state == CLOSED) {
			tcpclose(eptr->sock);
			if (eptr->rpacket) {
				csserv_delete_packet(eptr->rpacket);
			}
			if (eptr->wpacket) {
				csserv_delete_preserved(eptr->wpacket);
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
			pptr = eptr->outputhead;
			while (pptr) {
				if (pptr->packet) {
					free(pptr->packet);
				}
				paptr = pptr;
				pptr = pptr->next;
				delete paptr;
			}
			*kptr = eptr->next;
			delete eptr;
		} else {
			kptr = &(eptr->next);
		}
	}
}

uint32_t csserv_getlistenip() {
	TRACETHIS();
	return mylistenip;
}

uint16_t csserv_getlistenport() {
	TRACETHIS();
	return mylistenport;
}

void csserv_reload(void) {
	TRACETHIS();
	char *oldListenHost, *oldListenPort;
	int newlsock;

	oldListenHost = ListenHost;
	oldListenPort = ListenPort;
	ListenHost = cfg_getstr("CSSERV_LISTEN_HOST", "*");
	ListenPort = cfg_getstr("CSSERV_LISTEN_PORT", "9422");
	if (strcmp(oldListenHost, ListenHost) == 0 && strcmp(oldListenPort, ListenPort) == 0) {
		free(oldListenHost);
		free(oldListenPort);
		mfs_arg_syslog(LOG_NOTICE,
				"main server module: socket address hasn't changed (%s:%s)",
				ListenHost, ListenPort);
		return;
	}

	newlsock = tcpsocket();
	if (newlsock < 0) {
		mfs_errlog(LOG_WARNING,
				"main server module: socket address has changed, but can't create new socket");
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		return;
	}
	tcpnonblock(newlsock);
	tcpnodelay(newlsock);
	tcpreuseaddr(newlsock);
	if (tcpsetacceptfilter(newlsock) < 0 && errno != ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE, "main server module: can't set accept filter");
	}
	if (tcpstrlisten(newlsock, ListenHost, ListenPort, 100) < 0) {
		mfs_arg_errlog(LOG_ERR,
				"main server module: socket address has changed, but can't listen on socket (%s:%s)",
				ListenHost, ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		tcpclose(newlsock);
		return;
	}
	mfs_arg_syslog(LOG_NOTICE,
			"main server module: socket address has changed, now listen on %s:%s",
			ListenHost, ListenPort);
	free(oldListenHost);
	free(oldListenPort);
	tcpclose(lsock);
	lsock = newlsock;
}

int csserv_init(void) {
	TRACETHIS();
	ListenHost = cfg_getstr("CSSERV_LISTEN_HOST", "*");
	ListenPort = cfg_getstr("CSSERV_LISTEN_PORT", "9422");

	lsock = tcpsocket();
	if (lsock < 0) {
		mfs_errlog(LOG_ERR, "main server module: can't create socket");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	if (tcpsetacceptfilter(lsock) < 0 && errno != ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE, "main server module: can't set accept filter");
	}
	tcpresolve(ListenHost, ListenPort, &mylistenip, &mylistenport, 1);
	if (tcpnumlisten(lsock, mylistenip, mylistenport, 100) < 0) {
		mfs_errlog(LOG_ERR, "main server module: can't listen on socket");
		return -1;
	}
	mfs_arg_syslog(LOG_NOTICE, "main server module: listen on %s:%s", ListenHost, ListenPort);

	csservhead = NULL;
	main_reloadregister(csserv_reload);
	main_destructregister(csserv_term);
	main_pollregister(csserv_desc, csserv_serve);

	jpool = job_pool_new(NR_OF_BGJOBS_WORKERS, BGJOBSCNT, &jobfd);

	return 0;
}
