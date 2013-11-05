/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA.

   This file is part of MooseFS.

   MooseFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   MooseFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with MooseFS.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "config.h"

#include "chunkserver/masterconn.h"
// TODO: wtf?!
#define BGJOBS 1
#define BGJOBSCNT 1000

#include <time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>

#include <list>

#include "chunkserver/hddspacemgr.h"
#include "common/cfg.h"
#include "common/cstoma_communication.h"
#include "common/datapack.h"
#include "common/main.h"
#include "common/massert.h"
#include "common/MFSCommunication.h"
#include "common/packet.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "common/random.h"
#ifdef BGJOBS
#include "bgjobs.h"
#endif
#include "csserv.h"

#define MaxPacketSize 10000

// has to be less than MaxPacketSize on master side divided by 8
#define LOSTCHUNKLIMIT 25000

// mode
enum {FREE,CONNECTING,HEADER,DATA,KILL};

struct InputPacket {
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t *packet;
};

struct OutputPacket {
	std::vector<uint8_t> packet;
	uint32_t bytesSent;
	void swap(OutputPacket& other) {
		std::swap(packet, other.packet);
		std::swap(bytesSent, other.bytesSent);
	}
};

struct masterconn {
	int mode;
	int sock;
	int32_t pdescpos;
	uint32_t lastread,lastwrite;
	uint8_t hdrbuff[PacketHeader::kSize];
	InputPacket inputpacket;
	std::list<OutputPacket> outputPackets;
	uint32_t bindip;
	uint32_t masterip;
	uint16_t masterport;
	uint8_t masteraddrvalid;
};

static masterconn *masterconnsingleton=NULL;
#ifdef BGJOBS
static void *jpool;
static int jobfd;
static int32_t jobfdpdescpos;
#endif

// from config
// static uint32_t BackLogsNumber;
static char *MasterHost;
static char *MasterPort;
static char *BindHost;
static uint32_t Timeout;
static void* reconnect_hook;

static uint64_t stats_bytesout=0;
static uint64_t stats_bytesin=0;
static uint32_t stats_maxjobscnt=0;

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
	OutputPacket* outpacket = new OutputPacket();
	outpacket->packet.resize(size + PacketHeader::kSize);
	outpacket->bytesSent = 0;
	uint8_t *ptr = outpacket->packet.data();
	put32bit(&ptr, type);
	put32bit(&ptr, size);
	return outpacket;
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
	eptr->outputPackets.push_back(OutputPacket());
	std::swap(eptr->outputPackets.back(), *outputPacket);
	delete outputPacket;
}

void masterconn_create_attached_packet(masterconn *eptr, std::vector<uint8_t>& serializedPacket) {
	OutputPacket outputPacket;
	outputPacket.packet.swap(serializedPacket);
	outputPacket.bytesSent = 0;

	eptr->outputPackets.push_back(OutputPacket());
	std::swap(eptr->outputPackets.back(), outputPacket);
}

template<class... Data>
void masterconn_create_attached_moosefs_packet(masterconn *eptr,
		PacketHeader::Type type, const Data&... data) {
	std::vector<uint8_t> buffer;
	serializeMooseFsPacket(buffer, type, data...);
	masterconn_create_attached_packet(eptr, buffer);
}

void masterconn_sendregister(masterconn *eptr) {
	uint32_t myip;
	uint16_t myport;
	uint64_t usedspace,totalspace;
	uint64_t tdusedspace,tdtotalspace;
	uint32_t chunkcount,tdchunkcount;

	myip = csserv_getlistenip();
	myport = csserv_getlistenport();
	std::vector<uint8_t> serializedPacket;
	cstoma::registerHost::serialize(serializedPacket, myip, myport, Timeout, VERSHEX);
	masterconn_create_attached_packet(eptr, serializedPacket);
	hdd_get_chunks_begin();
	std::vector<ChunkWithVersionAndType> chunks;
	hdd_get_chunks_next_list_data(chunks);
	while (!chunks.empty()) {
		serializedPacket.resize(0);
		cstoma::registerChunks::serialize(serializedPacket, chunks);
		masterconn_create_attached_packet(eptr, serializedPacket);
		chunks.resize(0);
		hdd_get_chunks_next_list_data(chunks);
	}
	hdd_get_chunks_end();
	hdd_get_space(&usedspace,&totalspace,&chunkcount,&tdusedspace,&tdtotalspace,&tdchunkcount);
	serializedPacket.clear();
	cstoma::registerSpace::serialize(serializedPacket, usedspace, totalspace, chunkcount,
			tdusedspace, tdtotalspace, tdchunkcount);
	masterconn_create_attached_packet(eptr, serializedPacket);
}

void masterconn_check_hdd_reports() {
	masterconn *eptr = masterconnsingleton;
	uint32_t errorcounter;
	if (eptr->mode==DATA || eptr->mode==HEADER) {
		if (hdd_spacechanged()) {
			uint64_t usedspace,totalspace,tdusedspace,tdtotalspace;
			uint32_t chunkcount,tdchunkcount;
			hdd_get_space(&usedspace, &totalspace, &chunkcount, &tdusedspace, &tdtotalspace,
					&tdchunkcount);
			std::vector<uint8_t> buffer;
			masterconn_create_attached_moosefs_packet(
					eptr, CSTOMA_SPACE,
					usedspace, totalspace, chunkcount, tdusedspace, tdtotalspace, tdchunkcount);
		}
		errorcounter = hdd_errorcounter();
		while (errorcounter) {
			masterconn_create_attached_moosefs_packet(eptr, CSTOMA_ERROR_OCCURRED);
			errorcounter--;
		}
		std::vector<uint64_t> chunks;
		hdd_get_damaged_chunks(chunks);
		if (!chunks.empty()) {
			masterconn_create_attached_moosefs_packet(eptr, CSTOMA_CHUNK_DAMAGED, chunks);
		}
		chunks.clear();
		hdd_get_lost_chunks(chunks, LOSTCHUNKLIMIT);
		if (!chunks.empty()) {
			masterconn_create_attached_moosefs_packet(eptr, CSTOMA_CHUNK_LOST, chunks);
		}
		std::vector<ChunkWithVersionAndType> chunksWithVersionAndType;
		hdd_get_new_chunks(chunksWithVersionAndType);
		if (!chunksWithVersionAndType.empty()) {
			std::vector<uint8_t> buffer;
			cstoma::chunkNew::serialize(buffer, chunksWithVersionAndType);
			masterconn_create_attached_packet(eptr, buffer);
		}
	}
}

#ifdef BGJOBS
void masterconn_jobfinished(uint8_t status,void *packet) {
	uint8_t *ptr;
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode==DATA || eptr->mode==HEADER) {
		ptr = masterconn_get_packet_data(packet);
		ptr[8]=status;
		masterconn_attach_packet(eptr,packet);
	} else {
		masterconn_delete_packet(packet);
	}
}

void masterconn_chunkopfinished(uint8_t status,void *packet) {
	uint8_t *ptr;
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode==DATA || eptr->mode==HEADER) {
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
//	syslog(LOG_NOTICE,"job replication status: %" PRIu8,status);
	if (eptr->mode==DATA || eptr->mode==HEADER) {
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

#endif /* BGJOBS */

void masterconn_create(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t *ptr;
#ifdef BGJOBS
	void *packet;
#else /* BGJOBS */
	uint8_t status;
#endif /* BGJOBS */

	if (length!=8+4) {
		syslog(LOG_NOTICE,"MATOCS_CREATE - wrong size (%" PRIu32 "/12)",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
	version = get32bit(&data);
#ifdef BGJOBS
	packet = masterconn_create_detached_packet(CSTOMA_CREATE,8+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,chunkid);
	job_create(jpool,masterconn_jobfinished,packet,chunkid,version);
#else /* BGJOBS */
	status = hdd_create(chunkid,version);
	ptr = masterconn_create_attached_packet(eptr,CSTOMA_CREATE,8+1);
	put64bit(&ptr,chunkid);
	put8bit(&ptr,status);
#endif /* BGJOBS */
}

void masterconn_delete(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t *ptr;
#ifdef BGJOBS
	void *packet;
#else /* BGJOBS */
	uint8_t status;
#endif /* BGJOBS */

	if (length!=8+4) {
		syslog(LOG_NOTICE,"MATOCS_DELETE - wrong size (%" PRIu32 "/12)",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
	version = get32bit(&data);
#ifdef BGJOBS
	packet = masterconn_create_detached_packet(CSTOMA_DELETE,8+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,chunkid);
	job_delete(jpool,masterconn_jobfinished,packet,chunkid,version);
#else /* BGJOBS */
	status = hdd_delete(chunkid,version);
	ptr = masterconn_create_attached_packet(eptr,CSTOMA_DELETE,8+1);
	put64bit(&ptr,chunkid);
	put8bit(&ptr,status);
#endif /* BGJOBS */
}

void masterconn_setversion(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint32_t newversion;
	uint8_t *ptr;
#ifdef BGJOBS
	void *packet;
#else /* BGJOBS */
	uint8_t status;
#endif /* BGJOBS */

	if (length!=8+4+4) {
		syslog(LOG_NOTICE,"MATOCS_SET_VERSION - wrong size (%" PRIu32 "/16)",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
	newversion = get32bit(&data);
	version = get32bit(&data);
#ifdef BGJOBS
	packet = masterconn_create_detached_packet(CSTOMA_SET_VERSION,8+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,chunkid);
	job_version(jpool,masterconn_jobfinished,packet,chunkid,version,newversion);
#else /* BGJOBS */
	status = hdd_version(chunkid,version,newversion);
	ptr = masterconn_create_attached_packet(eptr,CSTOMA_SET_VERSION,8+1);
	put64bit(&ptr,chunkid);
	put8bit(&ptr,status);
#endif /* BGJOBS */
}

void masterconn_duplicate(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint64_t copychunkid;
	uint32_t copyversion;
	uint8_t *ptr;
#ifdef BGJOBS
	void *packet;
#else /* BGJOBS */
	uint8_t status;
#endif /* BGJOBS */

	if (length!=8+4+8+4) {
		syslog(LOG_NOTICE,"MATOCS_DUPLICATE - wrong size (%" PRIu32 "/24)",length);
		eptr->mode = KILL;
		return;
	}
	copychunkid = get64bit(&data);
	copyversion = get32bit(&data);
	chunkid = get64bit(&data);
	version = get32bit(&data);
#ifdef BGJOBS
	packet = masterconn_create_detached_packet(CSTOMA_DUPLICATE,8+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,copychunkid);
	job_duplicate(jpool,masterconn_jobfinished,packet,chunkid,version,version,copychunkid,copyversion);
#else /* BGJOBS */
	status = hdd_duplicate(chunkid,version,version,copychunkid,copyversion);
	ptr = masterconn_create_attached_packet(eptr,CSTOMA_DUPLICATE,8+1);
	put64bit(&ptr,copychunkid);
	put8bit(&ptr,status);
#endif /* BGJOBS */
}

void masterconn_truncate(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint32_t leng;
	uint32_t newversion;
	uint8_t *ptr;
#ifdef BGJOBS
	void *packet;
#else /* BGJOBS */
	uint8_t status;
#endif /* BGJOBS */

	if (length!=8+4+4+4) {
		syslog(LOG_NOTICE,"MATOCS_TRUNCATE - wrong size (%" PRIu32 "/20)",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
	leng = get32bit(&data);
	newversion = get32bit(&data);
	version = get32bit(&data);
#ifdef BGJOBS
	packet = masterconn_create_detached_packet(CSTOMA_TRUNCATE,8+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,chunkid);
	job_truncate(jpool,masterconn_jobfinished,packet,chunkid,version,newversion,leng);
#else /* BGJOBS */
	status = hdd_truncate(chunkid,version,newversion,leng);
	ptr = masterconn_create_attached_packet(eptr,CSTOMA_TRUNCATE,8+1);
	put64bit(&ptr,chunkid);
	put8bit(&ptr,status);
#endif /* BGJOBS */
}

void masterconn_duptrunc(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint64_t copychunkid;
	uint32_t copyversion;
	uint32_t leng;
	uint8_t *ptr;
#ifdef BGJOBS
	void *packet;
#else /* BGJOBS */
	uint8_t status;
#endif /* BGJOBS */

	if (length!=8+4+8+4+4) {
		syslog(LOG_NOTICE,"MATOCS_DUPTRUNC - wrong size (%" PRIu32 "/28)",length);
		eptr->mode = KILL;
		return;
	}
	copychunkid = get64bit(&data);
	copyversion = get32bit(&data);
	chunkid = get64bit(&data);
	version = get32bit(&data);
	leng = get32bit(&data);
#ifdef BGJOBS
	packet = masterconn_create_detached_packet(CSTOMA_DUPTRUNC,8+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,copychunkid);
	job_duptrunc(jpool,masterconn_jobfinished,packet,chunkid,version,version,copychunkid,copyversion,leng);
#else /* BGJOBS */
	status = hdd_duptrunc(chunkid,version,version,copychunkid,copyversion,leng);
	ptr = masterconn_create_attached_packet(eptr,CSTOMA_DUPTRUNC,8+1);
	put64bit(&ptr,copychunkid);
	put8bit(&ptr,status);
#endif /* BGJOBS */
}

void masterconn_chunkop(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version,newversion;
	uint64_t copychunkid;
	uint32_t copyversion;
	uint32_t leng;
	uint8_t *ptr;
#ifdef BGJOBS
	void *packet;
#else /* BGJOBS */
	uint8_t status;
#endif /* BGJOBS */

	if (length!=8+4+8+4+4+4) {
		syslog(LOG_NOTICE,"MATOCS_CHUNKOP - wrong size (%" PRIu32 "/32)",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
	version = get32bit(&data);
	newversion = get32bit(&data);
	copychunkid = get64bit(&data);
	copyversion = get32bit(&data);
	leng = get32bit(&data);
#ifdef BGJOBS
	packet = masterconn_create_detached_packet(CSTOMA_CHUNKOP,8+4+4+8+4+4+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	put32bit(&ptr,newversion);
	put64bit(&ptr,copychunkid);
	put32bit(&ptr,copyversion);
	put32bit(&ptr,leng);
	job_chunkop(jpool,masterconn_chunkopfinished,packet,chunkid,version,newversion,copychunkid,copyversion,leng);
#else /* BGJOBS */
	status = hdd_chunkop(chunkid,version,newversion,copychunkid,copyversion,leng);
	ptr = masterconn_create_attached_packet(eptr,CSTOMA_CHUNKOP,8+4+4+8+4+4+1);
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	put32bit(&ptr,newversion);
	put64bit(&ptr,copychunkid);
	put32bit(&ptr,copyversion);
	put32bit(&ptr,leng);
	put8bit(&ptr,status);
#endif /* BGJOBS */
}

#ifdef BGJOBS
void masterconn_replicate(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint32_t ip;
	uint16_t port;
	uint8_t *ptr;
	void *packet;

	if (length!=8+4+4+2 && (length<12+18 || length>12+18*100 || (length-12)%18!=0)) {
		syslog(LOG_NOTICE,"MATOCS_REPLICATE - wrong size (%" PRIu32 "/18|12+n*18[n:1..100])",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
	version = get32bit(&data);
	packet = masterconn_create_detached_packet(CSTOMA_REPLICATE,8+4+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	if (length==8+4+4+2) {
		ip = get32bit(&data);
		port = get16bit(&data);
//		syslog(LOG_NOTICE,"start job replication (%08" PRIX64 ":%04" PRIX32 ":%04" PRIX32 ":%02" PRIX16 ")",chunkid,version,ip,port);
		job_replicate_simple(jpool,masterconn_replicationfinished,packet,chunkid,version,ip,port);
	} else {
		job_replicate(jpool,masterconn_replicationfinished,packet,chunkid,version,(length-12)/18,data);
	}
}

#else /* BGJOBS */

void masterconn_replicate(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t *ptr;

	syslog(LOG_WARNING,"This version of chunkserver can perform replication only in background, but was compiled without bgjobs");

	if (length!=8+4+4+2) {
		syslog(LOG_NOTICE,"MATOCS_REPLICATE - wrong size (%" PRIu32 "/18)",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
	version = get32bit(&data);

	ptr = masterconn_create_attached_packet(eptr,CSTOMA_REPLICATE,8+4+1);
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	put8bit(&ptr,ERROR_CANTCONNECT);	// any error
}
#endif

/*
void masterconn_structure_log(masterconn *eptr,const uint8_t *data,uint32_t length) {
	if (length<5) {
		syslog(LOG_NOTICE,"MATOCS_STRUCTURE_LOG - wrong size (%" PRIu32 "/4+data)",length);
		eptr->mode = KILL;
		return;
	}
	if (data[0]==0xFF && length<10) {
		syslog(LOG_NOTICE,"MATOCS_STRUCTURE_LOG - wrong size (%" PRIu32 "/9+data)",length);
		eptr->mode = KILL;
		return;
	}
	if (data[length-1]!='\0') {
		syslog(LOG_NOTICE,"MATOCS_STRUCTURE_LOG - invalid string");
		eptr->mode = KILL;
		return;
	}

	if (logfd==NULL) {
		logfd = fopen("changelog_csback.0.mfs","a");
	}

	if (data[0]==0xFF) {	// new version
		uint64_t version;
		data++;
		version = get64bit(&data);
		if (logfd) {
			fprintf(logfd,"%" PRIu64 ": %s\n",version,data);
		} else {
			syslog(LOG_NOTICE,"lost MFS change %" PRIu64 ": %s",version,data);
		}
	} else {	// old version
		uint32_t version;
		version = get32bit(&data);
		if (logfd) {
			fprintf(logfd,"%" PRIu32 ": %s\n",version,data);
		} else {
			syslog(LOG_NOTICE,"lost MFS change %" PRIu32 ": %s",version,data);
		}
	}

}

void masterconn_structure_log_rotate(masterconn *eptr,const uint8_t *data,uint32_t length) {
	char logname1[100],logname2[100];
	uint32_t i;
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"MATOCS_STRUCTURE_LOG_ROTATE - wrong size (%" PRIu32 "/0)",length);
		eptr->mode = KILL;
		return;
	}
	if (logfd!=NULL) {
		fclose(logfd);
		logfd=NULL;
	}
	if (BackLogsNumber>0) {
		for (i=BackLogsNumber ; i>0 ; i--) {
			snprintf(logname1,100,"changelog_csback.%" PRIu32 ".mfs",i);
			snprintf(logname2,100,"changelog_csback.%" PRIu32 ".mfs",i-1);
			rename(logname2,logname1);
		}
	} else {
		unlink("changelog_csback.0.mfs");
	}
}
*/

void masterconn_chunk_checksum(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t status;
	uint32_t checksum;

	if (length!=8+4) {
		syslog(LOG_NOTICE,"ANTOCS_CHUNK_CHECKSUM - wrong size (%" PRIu32 "/12)",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
	version = get32bit(&data);
	status = hdd_get_checksum(chunkid,version,&checksum);
	if (status!=STATUS_OK) {
		masterconn_create_attached_moosefs_packet(
				eptr, CSTOAN_CHUNK_CHECKSUM, chunkid, version, status);
	} else {
		masterconn_create_attached_moosefs_packet(
				eptr, CSTOAN_CHUNK_CHECKSUM, chunkid, version, checksum);
	}
}

void masterconn_gotpacket(masterconn *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
	switch (type) {
		case ANTOAN_NOP:
			break;
		case ANTOAN_UNKNOWN_COMMAND: // for future use
			break;
		case ANTOAN_BAD_COMMAND_SIZE: // for future use
			break;
		case MATOCS_CREATE:
			masterconn_create(eptr,data,length);
			break;
		case MATOCS_DELETE:
			masterconn_delete(eptr,data,length);
			break;
		case MATOCS_SET_VERSION:
			masterconn_setversion(eptr,data,length);
			break;
		case MATOCS_DUPLICATE:
			masterconn_duplicate(eptr,data,length);
			break;
		case MATOCS_REPLICATE:
			masterconn_replicate(eptr,data,length);
			break;
		case MATOCS_CHUNKOP:
			masterconn_chunkop(eptr,data,length);
			break;
		case MATOCS_TRUNCATE:
			masterconn_truncate(eptr,data,length);
			break;
		case MATOCS_DUPTRUNC:
			masterconn_duptrunc(eptr,data,length);
			break;
//		case MATOCS_STRUCTURE_LOG:
//			masterconn_structure_log(eptr,data,length);
//			break;
//		case MATOCS_STRUCTURE_LOG_ROTATE:
//			masterconn_structure_log_rotate(eptr,data,length);
//			break;
		case ANTOCS_CHUNK_CHECKSUM:
			masterconn_chunk_checksum(eptr,data,length);
			break;
		default:
			syslog(LOG_NOTICE,"got unknown message (type:%" PRIu32 ")",type);
			eptr->mode = KILL;
	}
}


void masterconn_term(void) {
//	syslog(LOG_INFO,"closing %s:%s",MasterHost,MasterPort);
	masterconn *eptr = masterconnsingleton;

	job_pool_delete(jpool);

	if (eptr->mode!=FREE && eptr->mode!=CONNECTING) {
		tcpclose(eptr->sock);

		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
	}

	delete eptr;
	masterconnsingleton = NULL;

	free(MasterHost);
	free(MasterPort);
	free(BindHost);
}

void masterconn_connected(masterconn *eptr) {
	tcpnodelay(eptr->sock);
	eptr->mode=HEADER;
	eptr->inputpacket.bytesleft = PacketHeader::kSize;
	eptr->inputpacket.startptr = eptr->hdrbuff;
	eptr->inputpacket.packet = NULL;

	masterconn_sendregister(eptr);
	eptr->lastread = eptr->lastwrite = main_time();
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
				mfs_arg_syslog(LOG_WARNING,"master connection module: localhost (%u.%u.%u.%u) can't be used for connecting with master (use ip address of network controller)",(mip>>24)&0xFF,(mip>>16)&0xFF,(mip>>8)&0xFF,mip&0xFF);
				return -1;
			}
		} else {
			mfs_arg_syslog(LOG_WARNING,"master connection module: can't resolve master host/port (%s:%s)",MasterHost,MasterPort);
			return -1;
		}
	}
	eptr->sock=tcpsocket();
	if (eptr->sock<0) {
		mfs_errlog(LOG_WARNING,"master connection module: create socket error");
		return -1;
	}
	if (tcpnonblock(eptr->sock)<0) {
		mfs_errlog(LOG_WARNING,"master connection module: set nonblock error");
		tcpclose(eptr->sock);
		eptr->sock = -1;
		return -1;
	}
	if (eptr->bindip>0) {
		if (tcpnumbind(eptr->sock,eptr->bindip,0)<0) {
			mfs_errlog(LOG_WARNING,"master connection module: can't bind socket to given ip");
			tcpclose(eptr->sock);
			eptr->sock = -1;
			return -1;
		}
	}
	status = tcpnumconnect(eptr->sock,eptr->masterip,eptr->masterport);
	if (status<0) {
		mfs_errlog(LOG_WARNING,"master connection module: connect failed");
		tcpclose(eptr->sock);
		eptr->sock = -1;
		eptr->masteraddrvalid = 0;
		return -1;
	}
	if (status==0) {
		syslog(LOG_NOTICE,"connected to Master immediately");
		masterconn_connected(eptr);
	} else {
		eptr->mode = CONNECTING;
		syslog(LOG_NOTICE,"connecting ...");
	}
	return 0;
}

void masterconn_connecttest(masterconn *eptr) {
	int status;

	status = tcpgetstatus(eptr->sock);
	if (status) {
		mfs_errlog_silent(LOG_WARNING,"connection failed, error");
		tcpclose(eptr->sock);
		eptr->sock = -1;
		eptr->mode = FREE;
		eptr->masteraddrvalid = 0;
	} else {
		syslog(LOG_NOTICE,"connected to Master");
		masterconn_connected(eptr);
	}
}

void masterconn_read(masterconn *eptr) {
	int32_t i;
	for (;;) {
#ifdef BGJOBS
		if (job_pool_jobs_count(jpool)>=(BGJOBSCNT*9)/10) {
			return;
		}
#endif
		i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
		if (i==0) {
			syslog(LOG_NOTICE,"connection reset by Master");
			eptr->mode = KILL;
			return;
		}
		if (i<0) {
			if (errno!=EAGAIN) {
				mfs_errlog_silent(LOG_NOTICE,"read from Master error");
				eptr->mode = KILL;
			}
			return;
		}
		stats_bytesin+=i;
		eptr->inputpacket.startptr+=i;
		eptr->inputpacket.bytesleft-=i;

		if (eptr->inputpacket.bytesleft>0) {
			return;
		}

		PacketHeader header;
		deserializePacketHeader(eptr->hdrbuff, PacketHeader::kSize, header);
		if (eptr->mode==HEADER) {
			if (header.length > 0) {
				if (header.length > MaxPacketSize) {
					syslog(LOG_WARNING, "Master packet too long (%" PRIu32 "/%u)",
							header.length, MaxPacketSize);
					eptr->mode = KILL;
					return;
				}
				eptr->inputpacket.packet = (uint8_t*) malloc(header.length);
				passert(eptr->inputpacket.packet);
				eptr->inputpacket.bytesleft = header.length;
				eptr->inputpacket.startptr = eptr->inputpacket.packet;
				eptr->mode = DATA;
				continue;
			}
			eptr->mode = DATA;
		}

		if (eptr->mode==DATA) {

			eptr->mode=HEADER;
			eptr->inputpacket.bytesleft = PacketHeader::kSize;
			eptr->inputpacket.startptr = eptr->hdrbuff;

			masterconn_gotpacket(eptr, header.type, eptr->inputpacket.packet, header.length);

			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet=NULL;
		}
	}
}

void masterconn_write(masterconn *eptr) {
	int32_t i;
	while (!eptr->outputPackets.empty()) {
		auto& pack = eptr->outputPackets.front();
		i=write(eptr->sock, pack.packet.data() + pack.bytesSent,
				pack.packet.size() - pack.bytesSent);
		if (i<0) {
			if (errno!=EAGAIN) {
				mfs_errlog_silent(LOG_NOTICE,"write to Master error");
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
	}
}


void masterconn_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	masterconn *eptr = masterconnsingleton;

	eptr->pdescpos = -1;
	jobfdpdescpos = -1;

	if (eptr->mode==FREE || eptr->sock<0) {
		return;
	}
	if (eptr->mode==HEADER || eptr->mode==DATA) {
#ifdef BGJOBS
		pdesc[pos].fd = jobfd;
		pdesc[pos].events = POLLIN;
		jobfdpdescpos = pos;
		pos++;
		if (job_pool_jobs_count(jpool)<(BGJOBSCNT*9)/10) {
			pdesc[pos].fd = eptr->sock;
			pdesc[pos].events = POLLIN;
			eptr->pdescpos = pos;
			pos++;
		}
#else /* BGJOBS */
		pdesc[pos].fd = eptr->sock;
		pdesc[pos].events = POLLIN;
		eptr->pdescpos = pos;
		pos++;
#endif /* BGJOBS */
	}
	if (((eptr->mode==HEADER || eptr->mode==DATA) && !eptr->outputPackets.empty())
			|| eptr->mode==CONNECTING) {
		if (eptr->pdescpos>=0) {
			pdesc[eptr->pdescpos].events |= POLLOUT;
		} else {
			pdesc[pos].fd = eptr->sock;
			pdesc[pos].events = POLLOUT;
			eptr->pdescpos = pos;
			pos++;
		}
	}
	*ndesc = pos;
}

void masterconn_serve(struct pollfd *pdesc) {
	uint32_t now=main_time();
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
#ifdef BGJOBS
		if ((eptr->mode==HEADER || eptr->mode==DATA) && jobfdpdescpos>=0 && (pdesc[jobfdpdescpos].revents & POLLIN)) { // FD_ISSET(jobfd,rset)) {
			job_pool_check_jobs(jpool);
		}
#endif /* BGJOBS */
		if (eptr->pdescpos>=0) {
			if ((eptr->mode==HEADER || eptr->mode==DATA) && (pdesc[eptr->pdescpos].revents & POLLIN)) { // FD_ISSET(eptr->sock,rset)) {
				eptr->lastread = now;
				masterconn_read(eptr);
			}
			if ((eptr->mode==HEADER || eptr->mode==DATA) && (pdesc[eptr->pdescpos].revents & POLLOUT)) { // FD_ISSET(eptr->sock,wset)) {
				eptr->lastwrite = now;
				masterconn_write(eptr);
			}
			if ((eptr->mode==HEADER || eptr->mode==DATA) && eptr->lastread+Timeout<now) {
				eptr->mode = KILL;
			}
			if ((eptr->mode==HEADER || eptr->mode==DATA) && eptr->lastwrite+(Timeout/3)<now && eptr->outputPackets.empty()) {
				masterconn_create_attached_moosefs_packet(eptr, ANTOAN_NOP);
			}
		}
	}
#ifdef BGJOBS
	if (eptr->mode==HEADER || eptr->mode==DATA) {
		uint32_t jobscnt = job_pool_jobs_count(jpool);
		if (jobscnt>=stats_maxjobscnt) {
			stats_maxjobscnt=jobscnt;
		}
	}
#endif
	if (eptr->mode == KILL) {
#ifdef BGJOBS
		job_pool_disable_and_change_callback_all(jpool,masterconn_unwantedjobfinished);
#endif /* BGJOBS */
		tcpclose(eptr->sock);
		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
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

void masterconn_reload(void) {
	masterconn *eptr = masterconnsingleton;
	uint32_t ReconnectionDelay;

	free(MasterHost);
	free(MasterPort);
	free(BindHost);

	MasterHost = cfg_getstr("MASTER_HOST","mfsmaster");
	MasterPort = cfg_getstr("MASTER_PORT","9420");
	BindHost = cfg_getstr("BIND_HOST","*");

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
				mfs_arg_syslog(LOG_WARNING,"master connection module: localhost (%u.%u.%u.%u) can't be used for connecting with master (use ip address of network controller)",(mip>>24)&0xFF,(mip>>16)&0xFF,(mip>>8)&0xFF,mip&0xFF);
			}
		} else {
			mfs_arg_syslog(LOG_WARNING,"master connection module: can't resolve master host/port (%s:%s)",MasterHost,MasterPort);
		}
	} else {
		eptr->masteraddrvalid=0;
	}

	Timeout = cfg_getuint32("MASTER_TIMEOUT",60);

	ReconnectionDelay = cfg_getuint32("MASTER_RECONNECTION_DELAY",5);

	if (Timeout>65536) {
		Timeout=65535;
	}
	if (Timeout<10) {
		Timeout=10;
	}

	main_timechange(reconnect_hook,TIMEMODE_RUN_LATE,ReconnectionDelay,0);
}

int masterconn_init(void) {
	uint32_t ReconnectionDelay;
	masterconn *eptr;

	ReconnectionDelay = cfg_getuint32("MASTER_RECONNECTION_DELAY",5);
	MasterHost = cfg_getstr("MASTER_HOST","mfsmaster");
	MasterPort = cfg_getstr("MASTER_PORT","9420");
	BindHost = cfg_getstr("BIND_HOST","*");
	Timeout = cfg_getuint32("MASTER_TIMEOUT",60);
//	BackLogsNumber = cfg_getuint32("BACK_LOGS",50);

	if (Timeout>65536) {
		Timeout=65535;
	}
	if (Timeout<10) {
		Timeout=10;
	}
	eptr = masterconnsingleton = new masterconn;
	passert(eptr);

	eptr->masteraddrvalid = 0;
	eptr->mode = FREE;
	eptr->pdescpos = -1;
//	logfd = NULL;

	if (masterconn_initconnect(eptr)<0) {
		return -1;
	}

#ifdef BGJOBS
	jpool = job_pool_new(10,BGJOBSCNT,&jobfd);
	if (jpool==NULL) {
		return -1;
	}
#endif

	main_eachloopregister(masterconn_check_hdd_reports);
	reconnect_hook = main_timeregister(TIMEMODE_RUN_LATE,ReconnectionDelay,rndu32_ranged(ReconnectionDelay),masterconn_reconnect);
	main_destructregister(masterconn_term);
	main_pollregister(masterconn_desc,masterconn_serve);
	main_reloadregister(masterconn_reload);
	return 0;
}
