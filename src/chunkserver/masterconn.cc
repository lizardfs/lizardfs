/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o..

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

#include "common/platform.h"
#include "chunkserver/masterconn.h"

#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <list>

#include <list>

#include "chunkserver/bgjobs.h"
#include "chunkserver/hddspacemgr.h"
#include "chunkserver/network_main_thread.h"
#include "common/cfg.h"
#include "common/datapack.h"
#include "common/event_loop.h"
#include "common/goal.h"
#include "common/loop_watchdog.h"
#include "common/main.h"
#include "common/massert.h"
#include "common/moosefs_vector.h"
#include "common/output_packet.h"
#include "common/random.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "devtools/request_log.h"
#include "protocol/cstoma.h"
#include "protocol/input_packet.h"
#include "protocol/matocs.h"
#include "protocol/MFSCommunication.h"
#include "protocol/packet.h"

#define MaxPacketSize 10000

// has to be less than MaxPacketSize on master side divided by 8
#define LOSTCHUNKLIMIT 25000
// has to be less than MaxPacketSize on master side divided by 12
#define NEWCHUNKLIMIT 25000

#define BGJOBSCNT 1000

// mode
enum {FREE,CONNECTING,CONNECTED,KILL};

struct masterconn {
	masterconn()
	: mode(),
	  sock(),
	  pdescpos(),
	  lastread(),
	  lastwrite(),
	  inputPacket(MaxPacketSize),
	  outputPackets(),
	  bindip(),
	  masterip(),
	  masterport(),
	  masteraddrvalid() {}

	int mode;
	int sock;
	int32_t pdescpos;
	Timer lastread,lastwrite;
	InputPacket inputPacket;
	std::list<OutputPacket> outputPackets;
	uint32_t bindip;
	uint32_t masterip;
	uint16_t masterport;
	uint8_t masteraddrvalid;
};

static const uint64_t kSendStatusDelay = 5;

static masterconn *masterconnsingleton=NULL;
static void *jpool;
static int jobfd;
static int32_t jobfdpdescpos;

// from config
// static uint32_t BackLogsNumber;
static char *MasterHost;
static char *MasterPort;
static char *BindHost;
static uint32_t Timeout_ms;
static void* reconnect_hook;
static std::string gLabel;

static uint64_t stats_bytesout=0;
static uint64_t stats_bytesin=0;
static uint32_t stats_maxjobscnt=0;

static bool gEnableLoadFactor;

// static FILE *logfd;

void masterconn_stats(uint64_t *bin,uint64_t *bout,uint32_t *maxjobscnt) {
	*bin = stats_bytesin;
	*bout = stats_bytesout;
	*maxjobscnt = stats_maxjobscnt;
	stats_bytesin = 0;
	stats_bytesout = 0;
	stats_maxjobscnt = 0;
}

void* masterconn_create_detached_packet(uint32_t type,uint32_t size) {
	return new OutputPacket(PacketHeader(type, size));
}

uint8_t* masterconn_get_packet_data(void *packet) {
	OutputPacket* outpacket = (OutputPacket*)packet;
	return outpacket->packet.data() + PacketHeader::kSize;
}

void masterconn_delete_packet(void *packet) {
	OutputPacket* outputPacket = (OutputPacket*)packet;
	delete outputPacket;
}

void masterconn_attach_packet(masterconn *eptr, void* packet) {
	OutputPacket* outputPacket = (OutputPacket*) packet;
	eptr->outputPackets.emplace_back(std::move(*outputPacket));
	delete outputPacket;
}

void masterconn_create_attached_packet(masterconn *eptr, MessageBuffer serializedPacket) {
	eptr->outputPackets.emplace_back(std::move(serializedPacket));
}

template<class... Data>
void masterconn_create_attached_moosefs_packet(masterconn *eptr,
		PacketHeader::Type type, const Data&... data) {
	std::vector<uint8_t> buffer;
	serializeMooseFsPacket(buffer, type, data...);
	masterconn_create_attached_packet(eptr, std::move(buffer));
}

void masterconn_sendregisterlabel(masterconn *eptr) {
	if (eptr->mode == CONNECTED) {
		masterconn_create_attached_packet(eptr, cstoma::registerLabel::build(gLabel));
	}
}

void masterconn_sendregister(masterconn *eptr) {
	uint32_t myip;
	uint16_t myport;
	uint64_t usedspace,totalspace;
	uint64_t tdusedspace,tdtotalspace;
	uint32_t chunkcount,tdchunkcount;

	myip = mainNetworkThreadGetListenIp();
	myport = mainNetworkThreadGetListenPort();
	masterconn_create_attached_packet(eptr, cstoma::registerHost::build(myip, myport, Timeout_ms, LIZARDFS_VERSHEX));

	std::vector<ChunkWithVersionAndType> chunks;
	std::vector<ChunkWithType> recheck_list;

	hdd_get_chunks_begin();
	hdd_get_chunks_next_list_data(chunks, recheck_list);
	while (!chunks.empty()) {
		masterconn_create_attached_packet(eptr, cstoma::registerChunks::build(chunks));
		hdd_get_chunks_next_list_data(chunks, recheck_list);
	}
	hdd_get_chunks_end();

	hdd_get_chunks_next_list_data_recheck(chunks, recheck_list);
	while (!chunks.empty()) {
		masterconn_create_attached_packet(eptr, cstoma::registerChunks::build(chunks));
		hdd_get_chunks_next_list_data_recheck(chunks, recheck_list);
	}

	hdd_get_space(&usedspace,&totalspace,&chunkcount,&tdusedspace,&tdtotalspace,&tdchunkcount);
	auto registerSpace = cstoma::registerSpace::build(
			usedspace, totalspace, chunkcount, tdusedspace, tdtotalspace, tdchunkcount);
	masterconn_create_attached_packet(eptr, std::move(registerSpace));
	masterconn_sendregisterlabel(eptr);
}

void masterconn_check_hdd_reports() {
	masterconn *eptr = masterconnsingleton;
	uint32_t errorcounter;
	if (eptr->mode == CONNECTED) {
		if (hdd_spacechanged()) {
			uint64_t usedspace,totalspace,tdusedspace,tdtotalspace;
			uint32_t chunkcount,tdchunkcount;
			hdd_get_space(&usedspace, &totalspace, &chunkcount, &tdusedspace, &tdtotalspace,
					&tdchunkcount);
			masterconn_create_attached_moosefs_packet(
					eptr, CSTOMA_SPACE,
					usedspace, totalspace, chunkcount, tdusedspace, tdtotalspace, tdchunkcount);
		}
		errorcounter = hdd_errorcounter();
		while (errorcounter) {
			masterconn_create_attached_moosefs_packet(eptr, CSTOMA_ERROR_OCCURRED);
			errorcounter--;
		}

		std::vector<ChunkWithType> chunks_with_type;
		hdd_get_damaged_chunks(chunks_with_type, 1000);
		if (!chunks_with_type.empty()) {
			masterconn_create_attached_packet(eptr, cstoma::chunkDamaged::build(chunks_with_type));
		}

		hdd_get_lost_chunks(chunks_with_type, 1000);
		if (!chunks_with_type.empty()) {
			masterconn_create_attached_packet(eptr, cstoma::chunkLost::build(chunks_with_type));
		}

		std::vector<ChunkWithVersionAndType> chunks_with_version;
		hdd_get_new_chunks(chunks_with_version, 1000);
		if (!chunks_with_version.empty()) {
			masterconn_create_attached_packet(eptr, cstoma::chunkNew::build(chunks_with_version));
		}
	}
}

void masterconn_jobfinished(uint8_t status, void *packet) {
	uint8_t *ptr;
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode == CONNECTED) {
		ptr = masterconn_get_packet_data(packet);
		ptr[8]=status;
		masterconn_attach_packet(eptr,packet);
	} else {
		masterconn_delete_packet(packet);
	}
}

void masterconn_lizjobfinished(uint8_t status, void *packet) {
	OutputPacket* outputPacket = static_cast<OutputPacket*>(packet);
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode == CONNECTED) {
		cstoma::overwriteStatusField(outputPacket->packet, status);
		masterconn_attach_packet(eptr, packet);
	} else {
		masterconn_delete_packet(packet);
	}
}

void masterconn_chunkopfinished(uint8_t status,void *packet) {
	uint8_t *ptr;
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode == CONNECTED) {
		ptr = masterconn_get_packet_data(packet);
		ptr[32]=status;
		masterconn_attach_packet(eptr,packet);
	} else {
		masterconn_delete_packet(packet);
	}
}

void masterconn_replicationfinished(uint8_t status,void *packet) {
	uint8_t *ptr;
	masterconn *eptr = masterconnsingleton;
//      syslog(LOG_NOTICE,"job replication status: %" PRIu8,status);
	if (eptr->mode == CONNECTED) {
		ptr = masterconn_get_packet_data(packet);
		ptr[12]=status;
		masterconn_attach_packet(eptr,packet);
	} else {
		masterconn_delete_packet(packet);
	}
}

void masterconn_unwantedjobfinished(uint8_t status,void *packet) {
	(void)status;
	masterconn_delete_packet(packet);
}


void masterconn_create(masterconn */*eptr*/, const std::vector<uint8_t> &data) {
	uint64_t chunkId;
	ChunkPartType chunkType = slice_traits::standard::ChunkPartType();
	uint32_t chunkVersion;

	matocs::createChunk::deserialize(data, chunkId, chunkType, chunkVersion);
	OutputPacket *outputPacket = new OutputPacket;
	cstoma::createChunk::serialize(outputPacket->packet, chunkId, chunkType, LIZARDFS_STATUS_OK);
	job_create(jpool, masterconn_lizjobfinished, outputPacket, chunkId, chunkType, chunkVersion);
}

void masterconn_delete(masterconn */*eptr*/, const std::vector<uint8_t>& data) {
	uint64_t chunkId;
	uint32_t chunkVersion;
	ChunkPartType chunkType = slice_traits::standard::ChunkPartType();

	matocs::deleteChunk::deserialize(data, chunkId, chunkType, chunkVersion);
	OutputPacket* outputPacket = new OutputPacket;
	cstoma::deleteChunk::serialize(outputPacket->packet, chunkId, chunkType, 0);
	job_delete(jpool, masterconn_lizjobfinished, outputPacket, chunkId, chunkVersion, chunkType);
}

void masterconn_setversion(masterconn */*eptr*/, const std::vector<uint8_t>& data) {
	uint64_t chunkId;
	uint32_t chunkVersion;
	uint32_t newVersion;
	ChunkPartType chunkType = slice_traits::standard::ChunkPartType();

	matocs::setVersion::deserialize(data, chunkId, chunkType, chunkVersion, newVersion);
	OutputPacket* outputPacket = new OutputPacket;
	cstoma::setVersion::serialize(outputPacket->packet, chunkId, chunkType, 0);
	job_version(jpool, masterconn_lizjobfinished, outputPacket, chunkId, chunkVersion, chunkType,
			newVersion);
}

void masterconn_duplicate(masterconn* /*eptr*/,const std::vector<uint8_t>& data) {
	uint64_t newChunkId, oldChunkId;
	uint32_t newChunkVersion, oldChunkVersion;
	ChunkPartType chunkType = slice_traits::standard::ChunkPartType();

	matocs::duplicateChunk::deserialize(data, newChunkId, newChunkVersion, chunkType,
			oldChunkId, oldChunkVersion);
	OutputPacket* outputPacket = new OutputPacket;
	cstoma::duplicateChunk::serialize(outputPacket->packet, newChunkId, chunkType, 0);
	job_duplicate(jpool, masterconn_lizjobfinished, outputPacket, oldChunkId,
			oldChunkVersion, oldChunkVersion, chunkType, newChunkId, newChunkVersion);
}

void masterconn_truncate(masterconn */*eptr*/, const std::vector<uint8_t>& data) {
	uint64_t chunkId;
	ChunkPartType chunkType = slice_traits::standard::ChunkPartType();
	uint32_t version;
	uint32_t chunkLength;
	uint32_t newVersion;

	matocs::truncateChunk::deserialize(data, chunkId, chunkType, chunkLength, newVersion, version);
	OutputPacket* outputPacket = new OutputPacket;
	cstoma::truncate::serialize(outputPacket->packet, chunkId, chunkType, 0);
	job_truncate(jpool, masterconn_lizjobfinished, outputPacket, chunkId, chunkType, version,
			newVersion, chunkLength);
}

void masterconn_duptrunc(masterconn* /*eptr*/, const std::vector<uint8_t>& data) {
	uint64_t chunkId, copyChunkId;
	uint32_t chunkVersion, copyChunkVersion;
	ChunkPartType chunkType = slice_traits::standard::ChunkPartType();
	uint32_t newLength;

	matocs::duptruncChunk::deserialize(data, copyChunkId, copyChunkVersion,
			chunkType, chunkId, chunkVersion, newLength);
	OutputPacket* outputPacket = new OutputPacket;
	cstoma::duptruncChunk::serialize(outputPacket->packet, copyChunkId, chunkType, 0);
	job_duptrunc(jpool, masterconn_lizjobfinished, outputPacket, chunkId,
			chunkVersion, chunkVersion, chunkType, copyChunkId, copyChunkVersion, newLength);
}

void masterconn_chunkop(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version,newversion;
	uint64_t copychunkid;
	uint32_t copyversion;
	uint32_t leng;
	uint8_t *ptr;
	void *packet;

	if (length!=8+4+8+4+4+4) {
		lzfs_pretty_syslog(LOG_NOTICE,"MATOCS_CHUNKOP - wrong size (%" PRIu32 "/32)",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
	version = get32bit(&data);
	newversion = get32bit(&data);
	copychunkid = get64bit(&data);
	copyversion = get32bit(&data);
	leng = get32bit(&data);
	packet = masterconn_create_detached_packet(CSTOMA_CHUNKOP,8+4+4+8+4+4+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	put32bit(&ptr,newversion);
	put64bit(&ptr,copychunkid);
	put32bit(&ptr,copyversion);
	put32bit(&ptr,leng);
	job_chunkop(jpool, masterconn_chunkopfinished, packet, chunkid, version,
			slice_traits::standard::ChunkPartType(), newversion, copychunkid, copyversion, leng);
}

void masterconn_replicate(const std::vector<uint8_t>& data) {
	uint64_t chunkId;
	ChunkPartType chunkType = slice_traits::standard::ChunkPartType();
	uint32_t chunkVersion;
	uint32_t sourcesBufferSize;
	const uint8_t* sourcesBuffer;

	matocs::replicateChunk::deserializePartial(data,
			chunkId, chunkVersion, chunkType, sourcesBuffer);
	sourcesBufferSize = data.size() - (sourcesBuffer - data.data());

	OutputPacket* outputPacket = new OutputPacket;
	cstoma::replicateChunk::serialize(outputPacket->packet,
			chunkId, chunkType, LIZARDFS_STATUS_OK, chunkVersion);
	lzfs_silent_syslog(LOG_DEBUG, "cs.matocs.replicate %" PRIu64, chunkId);
	if (hdd_scans_in_progress()) {
		// Folder scan in progress - replication is not possible
		masterconn_lizjobfinished(LIZARDFS_ERROR_WAITING, outputPacket);
	} else {
		job_replicate(jpool, masterconn_lizjobfinished, outputPacket,
				chunkId, chunkVersion, chunkType, sourcesBufferSize, sourcesBuffer);
	}

}

void masterconn_legacy_replicate(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint32_t ip;
	uint16_t port;
	uint8_t *ptr;
	void *packet;

	if (length!=8+4+4+2 && (length<12+18 || length>12+18*100 || (length-12)%18!=0)) {
		lzfs_pretty_syslog(LOG_NOTICE,"MATOCS_REPLICATE - wrong size (%" PRIu32 "/18|12+n*18[n:1..100])",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
	version = get32bit(&data);
	lzfs_silent_syslog(LOG_DEBUG, "cs.matocs.replicate %" PRIu64, chunkid);
	packet = masterconn_create_detached_packet(CSTOMA_REPLICATE,8+4+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);

	if (hdd_scans_in_progress()) {
		masterconn_replicationfinished(LIZARDFS_ERROR_WAITING, packet);
	} else if (length==8+4+4+2) {
		ip = get32bit(&data);
		port = get16bit(&data);
//              syslog(LOG_NOTICE,"start job replication (%08" PRIX64 ":%04" PRIX32 ":%04" PRIX32 ":%02" PRIX16 ")",chunkid,version,ip,port);
		job_legacy_replicate_simple(jpool,masterconn_replicationfinished,packet,chunkid,version,ip,port);
	} else {
		job_legacy_replicate(jpool,masterconn_replicationfinished,packet,chunkid,version,(length-12)/18,data);
	}
}

void masterconn_gotpacket(masterconn *eptr, PacketHeader header, const MessageBuffer& message) try {
	switch (header.type) {
		case ANTOAN_NOP:
			break;
		case ANTOAN_UNKNOWN_COMMAND: // for future use
			break;
		case ANTOAN_BAD_COMMAND_SIZE: // for future use
			break;
		case LIZ_MATOCS_CREATE_CHUNK:
			masterconn_create(eptr, message);
			break;
		case LIZ_MATOCS_DELETE_CHUNK:
			masterconn_delete(eptr, message);
			break;
		case LIZ_MATOCS_SET_VERSION:
			masterconn_setversion(eptr, message);
			break;
		case LIZ_MATOCS_DUPLICATE_CHUNK:
			masterconn_duplicate(eptr, message);
			break;
		case MATOCS_REPLICATE:
			masterconn_legacy_replicate(eptr, message.data(), message.size());
			break;
		case LIZ_MATOCS_REPLICATE_CHUNK:
			masterconn_replicate(message);
			break;
		case MATOCS_CHUNKOP:
			masterconn_chunkop(eptr, message.data(), message.size());
			break;
		case LIZ_MATOCS_TRUNCATE:
			masterconn_truncate(eptr, message);
			break;
		case LIZ_MATOCS_DUPTRUNC_CHUNK:
			masterconn_duptrunc(eptr, message);
			break;
//              case MATOCS_STRUCTURE_LOG:
//                      masterconn_structure_log(eptr, message.data(), message.size());
//                      break;
//              case MATOCS_STRUCTURE_LOG_ROTATE:
//                      masterconn_structure_log_rotate(eptr, message.data(), message.size());
//                      break;
		default:
			lzfs_pretty_syslog(LOG_NOTICE,"got unknown message (type:%" PRIu32 ")", header.type);
			eptr->mode = KILL;
	}
} catch (IncorrectDeserializationException& e) {
	lzfs_pretty_syslog(LOG_NOTICE,
			"chunkserver <-> master module: got inconsistent message "
			"(type:%" PRIu32 ", length:%" PRIu32"), %s",
			header.type, uint32_t(message.size()), e.what());
	eptr->mode = KILL;
}


void masterconn_term(void) {
//      syslog(LOG_INFO,"closing %s:%s",MasterHost,MasterPort);
	masterconn *eptr = masterconnsingleton;

	job_pool_delete(jpool);

	if (eptr->mode!=FREE && eptr->mode!=CONNECTING) {
		tcpclose(eptr->sock);
		eptr->inputPacket.reset();
	}

	delete eptr;
	masterconnsingleton = NULL;

	free(MasterHost);
	free(MasterPort);
	free(BindHost);
}

void masterconn_connected(masterconn *eptr) {
	tcpnodelay(eptr->sock);
	eptr->mode = CONNECTED;
	eptr->inputPacket.reset();

	masterconn_sendregister(eptr);
	eptr->lastread.reset();
	eptr->lastwrite.reset();
}

int masterconn_initconnect(masterconn *eptr) {
	int status;
	if (eptr->masteraddrvalid==0) {
		uint32_t mip,bip;
		uint16_t mport;
		if (tcpresolve(BindHost,NULL,&bip,NULL,1)<0) {
			bip = 0;
		}
		eptr->bindip = bip;
		if (tcpresolve(MasterHost,MasterPort,&mip,&mport,0)>=0) {
			if ((mip&0xFF000000)!=0x7F000000) {
				eptr->masterip = mip;
				eptr->masterport = mport;
				eptr->masteraddrvalid = 1;
			} else {
				lzfs_pretty_syslog(LOG_WARNING,"master connection module: localhost (%u.%u.%u.%u) can't be used for connecting with master (use ip address of network controller)",(mip>>24)&0xFF,(mip>>16)&0xFF,(mip>>8)&0xFF,mip&0xFF);
				return -1;
			}
		} else {
			lzfs_pretty_syslog(LOG_WARNING,"master connection module: can't resolve master host/port (%s:%s)",MasterHost,MasterPort);
			return -1;
		}
	}
	eptr->sock=tcpsocket();
	if (eptr->sock<0) {
		lzfs_pretty_errlog(LOG_WARNING,"master connection module: create socket error");
		return -1;
	}
	if (tcpnonblock(eptr->sock)<0) {
		lzfs_pretty_errlog(LOG_WARNING,"master connection module: set nonblock error");
		tcpclose(eptr->sock);
		eptr->sock = -1;
		return -1;
	}
	if (eptr->bindip>0) {
		if (tcpnumbind(eptr->sock,eptr->bindip,0)<0) {
			lzfs_pretty_errlog(LOG_WARNING,"master connection module: can't bind socket to given ip");
			tcpclose(eptr->sock);
			eptr->sock = -1;
			return -1;
		}
	}
	status = tcpnumconnect(eptr->sock,eptr->masterip,eptr->masterport);
	if (status<0) {
		lzfs_pretty_errlog(LOG_WARNING,"master connection module: connect failed");
		tcpclose(eptr->sock);
		eptr->sock = -1;
		eptr->masteraddrvalid = 0;
		return -1;
	}
	if (status==0) {
		lzfs_pretty_syslog(LOG_NOTICE,"connected to Master immediately");
		masterconn_connected(eptr);
	} else {
		eptr->mode = CONNECTING;
		lzfs_pretty_syslog_attempt(LOG_NOTICE,"connecting to Master");
	}
	return 0;
}

void masterconn_connecttest(masterconn *eptr) {
	int status;

	status = tcpgetstatus(eptr->sock);
	if (status) {
		lzfs_silent_errlog(LOG_WARNING,"connection failed, error");
		tcpclose(eptr->sock);
		eptr->sock = -1;
		eptr->mode = FREE;
		eptr->masteraddrvalid = 0;
	} else {
		lzfs_pretty_syslog(LOG_NOTICE,"connected to Master");
		masterconn_connected(eptr);
	}
}

void masterconn_read(masterconn *eptr) {
	ActiveLoopWatchdog watchdog(std::chrono::milliseconds(20));

	watchdog.start();
	while (eptr->mode != KILL) {
		if (job_pool_jobs_count(jpool) >= (BGJOBSCNT * 9) / 10) {
			return;
		}
		uint32_t bytesToRead = eptr->inputPacket.bytesToBeRead();
		ssize_t ret = read(eptr->sock, eptr->inputPacket.pointerToBeReadInto(), bytesToRead);
		if (ret == 0) {
			lzfs_silent_syslog(LOG_NOTICE, "connection reset by Master");
			eptr->mode = KILL;
			return;
		} else if (ret < 0) {
			if (errno != EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE, "read from Master error");
				eptr->mode = KILL;
			}
			return;
		}

		stats_bytesin += ret;
		try {
			eptr->inputPacket.increaseBytesRead(ret);
		} catch (InputPacketTooLongException& ex) {
			lzfs_silent_syslog(LOG_WARNING, "reading from master: %s", ex.what());
			eptr->mode = KILL;
			return;
		}
		if (ret == bytesToRead && !eptr->inputPacket.hasData()) {
			// there might be more data to read in socket's buffer
			continue;
		} else if (!eptr->inputPacket.hasData()) {
			return;
		}

		masterconn_gotpacket(eptr, eptr->inputPacket.getHeader(), eptr->inputPacket.getData());
		eptr->inputPacket.reset();

		if (watchdog.expired()) {
			break;
		}
	}
}

void masterconn_write(masterconn *eptr) {
	ActiveLoopWatchdog watchdog(std::chrono::milliseconds(20));
	int32_t i;

	watchdog.start();
	while (!eptr->outputPackets.empty()) {
		OutputPacket& pack = eptr->outputPackets.front();
		i=write(eptr->sock, pack.packet.data() + pack.bytesSent,
				pack.packet.size() - pack.bytesSent);
		if (i<0) {
			if (errno!=EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE,"write to Master error");
				eptr->mode = KILL;
			}
			return;
		}
		stats_bytesout+=i;
		pack.bytesSent += i;
		if (pack.packet.size() != pack.bytesSent) {
			return;
		}
		eptr->outputPackets.pop_front();

		if (watchdog.expired()) {
			break;
		}
	}
}


void masterconn_desc(std::vector<pollfd> &pdesc) {
	LOG_AVG_TILL_END_OF_SCOPE0("master_desc");
	masterconn *eptr = masterconnsingleton;

	eptr->pdescpos = -1;
	jobfdpdescpos = -1;

	if (eptr->mode==FREE || eptr->sock<0) {
		return;
	}
	if (eptr->mode == CONNECTED) {
		pdesc.push_back({jobfd,POLLIN,0});
		jobfdpdescpos = pdesc.size() - 1;
		if (job_pool_jobs_count(jpool)<(BGJOBSCNT*9)/10) {
			pdesc.push_back({eptr->sock,POLLIN,0});
			eptr->pdescpos = pdesc.size() - 1;
		}
	}
	if (((eptr->mode == CONNECTED) && !eptr->outputPackets.empty())
			|| eptr->mode==CONNECTING) {
		if (eptr->pdescpos>=0) {
			pdesc[eptr->pdescpos].events |= POLLOUT;
		} else {
			pdesc.push_back({eptr->sock,POLLOUT,0});
			eptr->pdescpos = pdesc.size() - 1;
		}
	}
}

void masterconn_send_status() {
	static uint8_t prev_factor = 0;
	masterconn *eptr = masterconnsingleton;

	if (gEnableLoadFactor) {
		uint8_t load_factor = hdd_get_load_factor();
		if (eptr->mode == CONNECTED && load_factor != prev_factor) {
			masterconn_create_attached_packet(eptr,
				cstoma::status::build(load_factor));
			prev_factor = load_factor;
		}
	}
}

void masterconn_serve(const std::vector<pollfd> &pdesc) {
	LOG_AVG_TILL_END_OF_SCOPE0("master_serve");
	masterconn *eptr = masterconnsingleton;

	if (eptr->pdescpos>=0 && (pdesc[eptr->pdescpos].revents & (POLLHUP | POLLERR))) {
		if (eptr->mode==CONNECTING) {
			masterconn_connecttest(eptr);
		} else {
			eptr->mode = KILL;
		}
	}
	if (eptr->mode==CONNECTING) {
		if (eptr->sock>=0 && eptr->pdescpos>=0 && (pdesc[eptr->pdescpos].revents & POLLOUT)) { // FD_ISSET(eptr->sock,wset)) {
			masterconn_connecttest(eptr);
		}
	} else {
		if ((eptr->mode == CONNECTED) && jobfdpdescpos>=0 && (pdesc[jobfdpdescpos].revents & POLLIN)) { // FD_ISSET(jobfd,rset)) {
			job_pool_check_jobs(jpool);
		}
		if (eptr->pdescpos>=0) {
			if ((eptr->mode == CONNECTED) && (pdesc[eptr->pdescpos].revents & POLLIN)) { // FD_ISSET(eptr->sock,rset)) {
				eptr->lastread.reset();
				masterconn_read(eptr);
			}
			if ((eptr->mode == CONNECTED) && (pdesc[eptr->pdescpos].revents & POLLOUT)) { // FD_ISSET(eptr->sock,wset)) {
				eptr->lastwrite.reset();
				masterconn_write(eptr);
			}
			if ((eptr->mode == CONNECTED) && eptr->lastread.elapsed_ms() > Timeout_ms) {
				eptr->mode = KILL;
			}
			if ((eptr->mode == CONNECTED) && eptr->lastwrite.elapsed_ms() > (Timeout_ms/3) && eptr->outputPackets.empty()) {
				masterconn_create_attached_moosefs_packet(eptr, ANTOAN_NOP);
			}
		}
	}
	if (eptr->mode == CONNECTED) {
		uint32_t jobscnt = job_pool_jobs_count(jpool);
		if (jobscnt>=stats_maxjobscnt) {
			stats_maxjobscnt=jobscnt;
		}
	}
	if (eptr->mode == KILL) {
		job_pool_disable_and_change_callback_all(jpool,masterconn_unwantedjobfinished);
		tcpclose(eptr->sock);
		eptr->inputPacket.reset();
		eptr->outputPackets.clear();
		eptr->mode = FREE;
	}
}

void masterconn_reconnect(void) {
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode==FREE) {
		masterconn_initconnect(eptr);
	}
}

static uint32_t get_cfg_timeout() {
	return 1000 * cfg_get_minmaxvalue<double>("MASTER_TIMEOUT", 60, 0.01, 1000 * 1000);
}

/// Read the label from configuration file and return true if it's changed to a valid one
bool masterconn_load_label() {
	std::string oldLabel = gLabel;
	gLabel = cfg_getstring("LABEL", MediaLabelManager::kWildcard);
	if (!MediaLabelManager::isLabelValid(gLabel)) {
		lzfs_pretty_syslog(LOG_WARNING,"invalid label '%s'", gLabel.c_str());
		return false;
	}
	return gLabel != oldLabel;
}

void masterconn_reload(void) {
	masterconn *eptr = masterconnsingleton;
	uint32_t ReconnectionDelay;

	free(MasterHost);
	free(MasterPort);
	free(BindHost);

	MasterHost = cfg_getstr("MASTER_HOST","mfsmaster");
	MasterPort = cfg_getstr("MASTER_PORT","9420");
	BindHost = cfg_getstr("BIND_HOST","*");

	gEnableLoadFactor = cfg_getuint32("ENABLE_LOAD_FACTOR", 0);

	if (eptr->masteraddrvalid && eptr->mode!=FREE) {
		uint32_t mip,bip;
		uint16_t mport;
		if (tcpresolve(BindHost,NULL,&bip,NULL,1)<0) {
			bip = 0;
		}
		if (eptr->bindip!=bip) {
			eptr->bindip = bip;
			eptr->mode = KILL;
		}
		if (tcpresolve(MasterHost,MasterPort,&mip,&mport,0)>=0) {
			if ((mip&0xFF000000)!=0x7F000000) {
				if (eptr->masterip!=mip || eptr->masterport!=mport) {
					eptr->masterip = mip;
					eptr->masterport = mport;
					eptr->mode = KILL;
				}
			} else {
				lzfs_pretty_syslog(LOG_WARNING,"master connection module: localhost (%u.%u.%u.%u) can't be used for connecting with master (use ip address of network controller)",(mip>>24)&0xFF,(mip>>16)&0xFF,(mip>>8)&0xFF,mip&0xFF);
			}
		} else {
			lzfs_pretty_syslog(LOG_WARNING,"master connection module: can't resolve master host/port (%s:%s)",MasterHost,MasterPort);
		}
	} else {
		eptr->masteraddrvalid=0;
	}

	Timeout_ms = get_cfg_timeout();

	ReconnectionDelay = cfg_getuint32("MASTER_RECONNECTION_DELAY",5);

	if (masterconn_load_label()) {
		masterconn_sendregisterlabel(eptr);
	}

	eventloop_timechange(reconnect_hook,TIMEMODE_RUN_LATE,ReconnectionDelay,0);
}

int masterconn_init(void) {
	uint32_t ReconnectionDelay;
	masterconn *eptr;

	ReconnectionDelay = cfg_getuint32("MASTER_RECONNECTION_DELAY",5);
	MasterHost = cfg_getstr("MASTER_HOST","mfsmaster");
	MasterPort = cfg_getstr("MASTER_PORT","9420");
	BindHost = cfg_getstr("BIND_HOST","*");
	Timeout_ms = get_cfg_timeout();
//      BackLogsNumber = cfg_getuint32("BACK_LOGS",50);
	gEnableLoadFactor = cfg_getuint32("ENABLE_LOAD_FACTOR", 0);

	if (!masterconn_load_label()) {
		return -1;
	}
	eptr = masterconnsingleton = new masterconn;
	passert(eptr);

	eptr->masteraddrvalid = 0;
	eptr->mode = FREE;
	eptr->pdescpos = -1;
//      logfd = NULL;

	if (masterconn_initconnect(eptr)<0) {
		return -1;
	}

	eventloop_eachloopregister(masterconn_check_hdd_reports);
	eventloop_timeregister(TIMEMODE_RUN_LATE, kSendStatusDelay, rnd_ranged<uint32_t>(kSendStatusDelay), masterconn_send_status);
	reconnect_hook = eventloop_timeregister(TIMEMODE_RUN_LATE,ReconnectionDelay,rnd_ranged<uint32_t>(ReconnectionDelay),masterconn_reconnect);
	eventloop_destructregister(masterconn_term);
	eventloop_pollregister(masterconn_desc,masterconn_serve);
	eventloop_reloadregister(masterconn_reload);
	return 0;
}

int masterconn_init_threads(void) {
	jpool = job_pool_new(10,BGJOBSCNT,&jobfd);
	if (jpool==NULL) {
		return -1;
	}
	return 0;
}
