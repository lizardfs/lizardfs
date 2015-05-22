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

#include "common/platform.h"
#include "master/matocsserv.h"

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
#include <set>
#include <vector>

#include "common/cfg.h"
#include "common/cstoma_communication.h"
#include "common/datapack.h"
#include "common/goal.h"
#include "common/hashfn.h"
#include "common/input_packet.h"
#include "common/lizardfs_version.h"
#include "common/main.h"
#include "common/massert.h"
#include "common/matocs_communication.h"
#include "common/MFSCommunication.h"
#include "common/mfserr.h"
#include "common/output_packet.h"
#include "common/packet.h"
#include "common/random.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "master/chunks.h"
#include "master/filesystem.h"
#include "master/get_servers_for_new_chunk.h"
#include "master/personality.h"

#define MaxPacketSize 500000000

// matocsserventry.mode
enum{KILL, CONNECTED};

struct matocsserventry {
	matocsserventry() : inputPacket(MaxPacketSize) {}

	uint8_t mode;
	int sock;
	int32_t pdescpos;
	Timer lastread,lastwrite;
	InputPacket inputPacket;
	std::list<OutputPacket> outputPackets;
	char *servstrip;                // human readable version of servip
	uint32_t version;
	uint32_t servip;                // ip to coonnect to
	uint16_t servport;              // port to connect to
	uint32_t timeout;               // communication timeout
	MediaLabel label;               // server label, empty if not set
	uint64_t usedspace;             // used hdd space in bytes
	uint64_t totalspace;            // total hdd space in bytes
	uint32_t chunkscount;
	uint64_t todelusedspace;
	uint64_t todeltotalspace;
	uint32_t todelchunkscount;
	uint32_t errorcounter;
	uint16_t rrepcounter;
	uint16_t wrepcounter;
	uint16_t delcounter;

	uint8_t incsdb;

	matocsserventry *next;
};

static matocsserventry *matocsservhead=NULL;
static int lsock;
static int32_t lsockpdescpos;

// from config
static char *ListenHost;
static char *ListenPort;

#define CSDBHASHSIZE 256
#define CSDBHASHFN(ip,port) (hash32((ip)^((port)<<16))%(CSDBHASHSIZE))

typedef struct csdbentry {
	uint32_t ip;
	uint16_t port;
	matocsserventry *eptr;
	struct csdbentry *next;
} csdbentry;

static csdbentry *csdbhash[CSDBHASHSIZE];

int matocsserv_csdb_new_connection(uint32_t ip,uint16_t port,matocsserventry *eptr) {
	uint32_t hash;
	csdbentry *csptr;

	hash = CSDBHASHFN(ip,port);
	for (csptr = csdbhash[hash] ; csptr ; csptr = csptr->next) {
		if (csptr->ip == ip && csptr->port == port) {
			if (csptr->eptr!=NULL) {
				return -1;
			}
			csptr->eptr = eptr;
			return 0;
		}
	}
	csptr = (csdbentry*) malloc(sizeof(csdbentry));
	passert(csptr);
	csptr->ip = ip;
	csptr->port = port;
	csptr->eptr = eptr;
	csptr->next = csdbhash[hash];
	csdbhash[hash] = csptr;
	return 1;
}

void matocsserv_csdb_lost_connection(uint32_t ip,uint16_t port) {
	uint32_t hash;
	csdbentry *csptr;

	hash = CSDBHASHFN(ip,port);
	for (csptr = csdbhash[hash] ; csptr ; csptr = csptr->next) {
		if (csptr->ip == ip && csptr->port == port) {
			csptr->eptr = NULL;
			return;
		}
	}
}

std::vector<ChunkserverListEntry> matocsserv_cservlist() {
	std::vector<ChunkserverListEntry> result;
	for (uint32_t hash = 0; hash < CSDBHASHSIZE; hash++) {
		for (csdbentry *csptr = csdbhash[hash]; csptr; csptr = csptr->next) {
			if (csptr->eptr != nullptr) {
				result.emplace_back(*csptr->eptr);
			} else {
				result.emplace_back(
						kDisconnectedChunkserverVersion,
						csptr->ip, csptr->port,
						0, 0, 0, 0, 0, 0, 0,
						kMediaLabelWildcard);
			}
		}
	}
	return result;
}

int matocsserv_csdb_remove_server(uint32_t ip,uint16_t port) {
	uint32_t hash;
	csdbentry *csptr,**cspptr;

	hash = CSDBHASHFN(ip,port);
	cspptr = csdbhash + hash;
	while ((csptr=*cspptr)) {
		if (csptr->ip == ip && csptr->port == port) {
			if (csptr->eptr!=NULL) {
				return -1;
			}
			*cspptr = csptr->next;
			free(csptr);
			return 1;
		} else {
			cspptr = &(csptr->next);
		}
	}
	return 0;
}

void matocsserv_csdb_init(void) {
	uint32_t hash;
	for (hash=0 ; hash<CSDBHASHSIZE ; hash++) {
		csdbhash[hash]=NULL;
	}
}

/* replications DB */

#define REPHASHSIZE 256
#define REPHASHFN(chid,ver) (((chid)^(ver)^((chid)>>8))%(REPHASHSIZE))

struct repsrc {
	matocsserventry *src;
	repsrc *next;
};

struct repdst {
	uint64_t chunkId;
	uint32_t chunkVersion;
	ChunkType chunkType;
	matocsserventry *destinationCs;
	repsrc *repsrcHead;
	repdst *next;
};

static repdst* rephash[REPHASHSIZE];
static repsrc *repsrcfreehead=NULL;
static repdst *repdstfreehead=NULL;

repsrc* matocsserv_repsrc_malloc() {
	repsrc *r;
	if (repsrcfreehead) {
		r = repsrcfreehead;
		repsrcfreehead = r->next;
	} else {
		r = (repsrc*)malloc(sizeof(repsrc));
		passert(r);
	}
	return r;
}

void matocsserv_repsrc_free(repsrc *r) {
	r->next = repsrcfreehead;
	repsrcfreehead = r;
}

repdst* matocsserv_repdst_malloc() {
	repdst *r;
	if (repdstfreehead) {
		r = repdstfreehead;
		repdstfreehead = r->next;
	} else {
		r = (repdst*)malloc(sizeof(repdst));
		passert(r);
	}
	return r;
}

void matocsserv_repdst_free(repdst *r) {
	r->next = repdstfreehead;
	repdstfreehead = r;
}

void matocsserv_replication_init(void) {
	uint32_t hash;
	for (hash=0 ; hash<REPHASHSIZE ; hash++) {
		rephash[hash]=NULL;
	}
	repsrcfreehead=NULL;
	repdstfreehead=NULL;
}

int matocsserv_replication_find(uint64_t chunkId, uint32_t chunkVersion,
		ChunkType chunkType, matocsserventry *dst) {
	uint32_t hash = REPHASHFN(chunkId, chunkVersion);
	for (repdst *replica = rephash[hash]; replica; replica = replica->next) {
		if (replica->chunkId == chunkId
				&& replica->chunkVersion == chunkVersion
				&& replica->chunkType == chunkType
				&& replica->destinationCs == dst) {
			return 1;
		}
	}
	return 0;
}

void matocsserv_replication_begin(uint64_t chunkId, uint32_t chunkVersion,
		ChunkType chunkType, matocsserventry *dst, uint8_t srccnt, matocsserventry* const *src) {
	if (srccnt == 0) {
		return;
	}

	uint32_t hash = REPHASHFN(chunkId, chunkVersion);
	repdst *replica;
	repsrc *replicaSource;

	replica = matocsserv_repdst_malloc();
	replica->chunkId = chunkId;
	replica->chunkVersion = chunkVersion;
	replica->chunkType = chunkType;
	replica->destinationCs = dst;
	replica->repsrcHead = NULL;
	replica->next = rephash[hash];
	rephash[hash] = replica;
	for (uint8_t i = 0 ; i < srccnt ; i++) {
		replicaSource = matocsserv_repsrc_malloc();
		replicaSource->src = src[i];
		replicaSource->next = replica->repsrcHead;
		replica->repsrcHead = replicaSource;
		static_cast<matocsserventry *>(src[i])->rrepcounter++;
	}
	static_cast<matocsserventry *>(dst)->wrepcounter++;
}

void matocsserv_replication_end(uint64_t chunkId, uint32_t chunkVersion,
		ChunkType chunkType, matocsserventry *destination) {
	uint32_t hash = REPHASHFN(chunkId, chunkVersion);
	repdst *replica, **replicaPointer;
	repsrc *replicaSource, *replicaSourceToDelete;

	replicaPointer = &(rephash[hash]);
	while ((replica = *replicaPointer) != NULL) {
		if (replica->chunkId == chunkId
				&& replica->chunkVersion == chunkVersion
				&& replica->chunkType == chunkType
				&& replica->destinationCs == destination) {
			replicaSource = replica->repsrcHead;
			while (replicaSource) {
				replicaSourceToDelete = replicaSource;
				replicaSource = replicaSource->next;
				static_cast<matocsserventry *>(replicaSourceToDelete->src)->rrepcounter--;
				matocsserv_repsrc_free(replicaSourceToDelete);
			}
			static_cast<matocsserventry *>(destination)->wrepcounter--;
			*replicaPointer = replica->next;
			matocsserv_repdst_free(replica);
		} else {
			replicaPointer = &(replica->next);
		}
	}
}

void matocsserv_replication_disconnected(matocsserventry *srv) {
	uint32_t hash;
	repdst *r, **rp;
	repsrc *rs, *rsdel, **rsp;

	for (hash = 0; hash < REPHASHSIZE; hash++) {
		rp = &(rephash[hash]);
		while ((r = *rp) != NULL) {
			if (r->destinationCs == srv) {
				rs = r->repsrcHead;
				while (rs) {
					rsdel = rs;
					rs = rs->next;
					rsdel->src->rrepcounter--;
					matocsserv_repsrc_free(rsdel);
				}
				srv->wrepcounter--;
				*rp = r->next;
				matocsserv_repdst_free(r);
			} else {
				rsp = &(r->repsrcHead);
				while ((rs = *rsp) != NULL) {
					if (rs->src == srv) {
						srv->rrepcounter--;
						*rsp = rs->next;
						matocsserv_repsrc_free(rs);
					} else {
						rsp = &(rs->next);
					}
				}
				rp = &(r->next);
			}
		}
	}
}

/* replication DB END */

struct servsort {
  double space;
  matocsserventry *ptr;
};
int matocsserv_space_compare(const void *a,const void *b) {
  const servsort *aa=(const servsort*)a,*bb=(const servsort*)b;
	if (aa->space > bb->space) {
		return 1;
	}
	if (aa->space < bb->space) {
		return -1;
	}
	return 0;
}

void matocsserv_usagedifference(double *minusage,double *maxusage,uint16_t *usablescount,uint16_t *totalscount) {
	matocsserventry *eptr;
	uint32_t j,k;
	double minspace=1.0,maxspace=0.0;
	double space;
	j = 0;
	k = 0;
	for (eptr = matocsservhead ; eptr && j<65535 && k<65535; eptr=eptr->next) {
		if (eptr->mode!=KILL) {
			if (eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace) {
				space = (double)(eptr->usedspace) / (double)(eptr->totalspace);
				if (j==0) {
					minspace = maxspace = space;
				} else if (space<minspace) {
					minspace = space;
				} else if (space>maxspace) {
					maxspace = space;
				}
				j++;
			}
			k++;
		}
	}
	if (usablescount) {
		*usablescount = j;
	}
	if (totalscount) {
		*totalscount = k;
	}
	if (j==0) {
		if (minusage) {
			*minusage = 1.0;
		}
		if (maxusage) {
			*maxusage = 0.0;
		}
	} else {
		if (minusage) {
			*minusage = minspace;
		}
		if (maxusage) {
			*maxusage = maxspace;
		}
	}
}

std::vector<ServerWithUsage> matocsserv_getservers_sorted() {
	std::vector<ServerWithUsage> result;
	for (matocsserventry* eptr = matocsservhead; eptr != nullptr; eptr=eptr->next) {
		if (eptr->mode != KILL
				&& eptr->totalspace > 0
				&& eptr->usedspace <= eptr->totalspace) {
			double usage = double(eptr->usedspace) / double(eptr->totalspace);
			result.emplace_back(eptr, usage, &eptr->label);
		}
	}
	std::sort(result.begin(), result.end(), [](const ServerWithUsage& a, const ServerWithUsage& b) {
		return a.diskUsage < b.diskUsage;
	});
	return result;
}

std::vector<std::pair<matocsserventry*, ChunkType>> matocsserv_getservers_for_new_chunk(
			uint8_t goalId) {
	static GoalMap<ChunkCreationHistory> history;
	GetServersForNewChunk getter;
	bool isXorGoal = goal::isXorGoal(goalId);
	// Add servers with weights which are proportional to total space on each server.
	for (matocsserventry* eptr = matocsservhead; eptr != nullptr ; eptr = eptr->next) {
		if (isXorGoal && eptr->version < kFirstXorVersion) {
			// can't store XOR chunks on chunkservers that don't support them
			continue;
		}
		if (eptr->mode != KILL && eptr->totalspace > 0 && eptr->usedspace <= eptr->totalspace
				&& (eptr->totalspace - eptr->usedspace) >= MFSCHUNKSIZE) {
			int64_t weight = eptr->totalspace / 1024U / 1024U; // weight = total space in MB
			getter.addServer(eptr, &eptr->label, weight);
		}
	}
	std::vector<std::pair<matocsserventry*, ChunkType>> ret;
	std::vector<ChunkType> chunkTypes;
	auto servers = getter.chooseServersForGoal(
			fs_get_goal_definition(goalId), history[goalId]);

	if (isXorGoal) {
		ChunkType::XorLevel level = goal::toXorLevel(goalId);
		if (servers.size() < level) {
			return ret; // do not create any parts if servers are missing
		}
		chunkTypes.reserve(level + 1);
		chunkTypes.push_back(ChunkType::getXorParityChunkType(level));
		for (int part = 1; part <= level; ++part) {
			chunkTypes.push_back(ChunkType::getXorChunkType(level, part));
		}
		std::random_shuffle(chunkTypes.begin(), chunkTypes.end());
		if (servers.size() == level) {
			chunkTypes.pop_back();
		}
	} else {
		chunkTypes.assign(servers.size(), ChunkType::getStandardChunkType());
	}
	for (uint32_t i = 0; i < servers.size(); ++i) {
		ret.emplace_back(servers[i], chunkTypes[i]);
	}
	return ret;
}

uint16_t matocsserv_getservers_lessrepl(const MediaLabel& label, uint16_t replicationWriteLimit,
		matocsserventry* servers[65535], uint16_t* totalMatching, uint16_t* returnedMatching) {
	uint32_t serverCount = 0;
	*totalMatching = 0;
	*returnedMatching = 0;
	for (matocsserventry* eptr = matocsservhead; eptr && serverCount < 65535; eptr = eptr->next) {
		if (eptr->mode == KILL
				|| eptr->totalspace == 0
				|| eptr->usedspace > eptr->totalspace
				|| (eptr->totalspace - eptr->usedspace) <= (eptr->totalspace / 100)) {
			// Skip full and disconnected chunkservers
			continue;
		}
		bool matchesRequestedLabel = false;
		if (label != kMediaLabelWildcard && eptr->label == label) {
			++(*totalMatching);
			matchesRequestedLabel = true;
		}
		if (eptr->wrepcounter < replicationWriteLimit) {
			servers[serverCount] = eptr;
			serverCount++;
			if (matchesRequestedLabel) {
				++(*returnedMatching);
			}
		}
	}
	std::random_shuffle(servers, servers + serverCount);
	if (*returnedMatching > 0) {
		// Move servers matching the requested label to the front of the servers array
		std::partition(servers, servers + serverCount, [&label](matocsserventry* cs) {
			return cs->label == label;
		});
	}
	return serverCount;
}

const MediaLabel& matocsserv_get_label(matocsserventry* e) {
	return e->label;
}

double matocsserv_get_usage(matocsserventry* eptr) {
	if (eptr->usedspace > eptr->totalspace) {
		return 1.0;
	} else {
		return double(eptr->usedspace) / double(eptr->totalspace);
	}
}

void matocsserv_getspace(uint64_t *totalspace,uint64_t *availspace) {
	matocsserventry *eptr;
	uint64_t tspace,uspace;
	tspace = 0;
	uspace = 0;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0) {
			tspace += eptr->totalspace;
			uspace += eptr->usedspace;
		}
	}
	*totalspace = tspace;
	*availspace = tspace-uspace;
}

const char* matocsserv_getstrip(matocsserventry *eptr) {
	static const char *empty = "???";
	if (eptr->mode!=KILL && eptr->servstrip) {
		return eptr->servstrip;
	}
	return empty;
}

int matocsserv_getlocation(matocsserventry *eptr,uint32_t *servip,uint16_t *servport,
		MediaLabel** label) {
	if (eptr->mode!=KILL) {
		*servip = eptr->servip;
		*servport = eptr->servport;
		*label = &(eptr->label);
		return 0;
	}
	return -1;
}


uint16_t matocsserv_replication_write_counter(matocsserventry *eptr) {
	return eptr->wrepcounter;
}

uint16_t matocsserv_replication_read_counter(matocsserventry *eptr) {
	return eptr->rrepcounter;
}

uint16_t matocsserv_deletion_counter(matocsserventry *eptr) {
	return eptr->delcounter;
}

char* matocsserv_makestrip(uint32_t ip) {
	uint8_t *ptr,pt[4];
	uint32_t l,i;
	char *optr;
	ptr = pt;
	put32bit(&ptr,ip);
	l=0;
	for (i=0 ; i<4 ; i++) {
		if (pt[i]>=100) {
			l+=3;
		} else if (pt[i]>=10) {
			l+=2;
		} else {
			l+=1;
		}
	}
	l+=4;
	optr = (char*) malloc(l);
	passert(optr);
	snprintf(optr,l,"%" PRIu8 ".%" PRIu8 ".%" PRIu8 ".%" PRIu8,pt[0],pt[1],pt[2],pt[3]);
	optr[l-1]=0;
	return optr;
}

uint8_t* matocsserv_createpacket(matocsserventry *eptr,uint32_t type,uint32_t size) {
	eptr->outputPackets.emplace_back(PacketHeader(type, size));
	return eptr->outputPackets.back().packet.data() + PacketHeader::kSize;
}

/* for future use */
int matocsserv_send_chunk_checksum(matocsserventry *eptr,uint64_t chunkid,uint32_t version) {
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,ANTOCS_CHUNK_CHECKSUM,8+4);
		put64bit(&data,chunkid);
		put32bit(&data,version);
	}
	return 0;
}
/* for future use */
void matocsserv_got_chunk_checksum(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version,checksum;
	uint8_t status;
	if (length!=8+4+1 && length!=8+4+4) {
		syslog(LOG_NOTICE,"CSTOAN_CHUNK_CHECKSUM - wrong size (%" PRIu32 "/13|16)",length);
		eptr->mode=KILL;
		return ;
	}
	passert(data);
	chunkid = get64bit(&data);
	version = get32bit(&data);
	if (length==8+4+1) {
		status = get8bit(&data);
		syslog(LOG_NOTICE,"(%s:%" PRIu16 ") chunk: %016" PRIX64 " calculate checksum status: %s",eptr->servstrip,eptr->servport,chunkid,mfsstrerr(status));
	} else {
		checksum = get32bit(&data);
		syslog(LOG_NOTICE,"(%s:%" PRIu16 ") chunk: %016" PRIX64 " calculate checksum: %08" PRIX32,eptr->servstrip,eptr->servport,chunkid,checksum);
	}
	(void)version;
}

int matocsserv_send_createchunk(matocsserventry *eptr, uint64_t chunkId, ChunkType chunkType,
		uint32_t chunkVersion) {
	if (eptr->mode != KILL) {
		eptr->outputPackets.push_back(OutputPacket());
		if (eptr->version < kFirstXorVersion) {
			// send old packet when chunkserver doesn't support xor chunks
			sassert(chunkType.isStandardChunkType());
			serializeMooseFsPacket(eptr->outputPackets.back().packet, MATOCS_CREATE, chunkId,
					chunkVersion);
		} else {
			matocs::createChunk::serialize(eptr->outputPackets.back().packet, chunkId, chunkType,
					chunkVersion);
		}
	}
	return 0;
}

void matocsserv_got_createchunk_status(matocsserventry *eptr, const std::vector<uint8_t> &data) {
	uint64_t chunkId;
	ChunkType chunkType = ChunkType::getStandardChunkType();
	uint8_t status;
	if (eptr->version < kFirstXorVersion) {
		// get old packet when chunkserver doesn't support xor chunks
		deserializeAllMooseFsPacketDataNoHeader(data, chunkId, status);
	}
	else {
		cstoma::createChunk::deserialize(data, chunkId, chunkType, status);
	}
	chunk_got_create_status(eptr, chunkId, chunkType, status);
	if (status != 0) {
		syslog(LOG_NOTICE,"(%s:%" PRIu16 ") chunk: %016" PRIX64 " creation status: %s",
				eptr->servstrip, eptr->servport, chunkId, mfsstrerr(status));
	}
}

int matocsserv_send_deletechunk(matocsserventry *eptr, uint64_t chunkId, uint32_t chunkVersion,
		ChunkType chunkType) {
	if (eptr->mode != KILL) {
		eptr->outputPackets.push_back(OutputPacket());
		if (eptr->version < kFirstXorVersion) {
			// send old packet when chunkserver doesn't support xor chunks
			sassert(chunkType == ChunkType::getStandardChunkType());
			serializeMooseFsPacket(eptr->outputPackets.back().packet, MATOCS_DELETE,
					chunkId, chunkVersion);
		}
		else {
			matocs::deleteChunk::serialize(eptr->outputPackets.back().packet,
					chunkId, chunkType, chunkVersion);
		}
		eptr->delcounter++;
	}
	return 0;
}

void matocsserv_got_deletechunk_status(matocsserventry *eptr, const std::vector<uint8_t>& data) {
	uint64_t chunkId;
	ChunkType chunkType = ChunkType::getStandardChunkType();
	uint8_t status;

	if (eptr->version < kFirstXorVersion) {
		deserializeAllMooseFsPacketDataNoHeader(data, chunkId, status);
	} else {
		cstoma::deleteChunk::deserialize(data, chunkId, chunkType, status);
	}

	chunk_got_delete_status(eptr, chunkId, chunkType, status);
	eptr->delcounter--;
	if (status != 0) {
		syslog(LOG_NOTICE,"(%s:%" PRIu16 ") chunk: %016" PRIX64 " deletion status: %s",
				eptr->servstrip, eptr->servport, chunkId, mfsstrerr(status));
	}
}

int matocsserv_send_replicatechunk(matocsserventry *eptr, uint64_t chunkid, uint32_t version, matocsserventry *srceptr) {
	uint8_t *data;

	if (matocsserv_replication_find(chunkid, version, ChunkType::getStandardChunkType(), eptr)) {
		return -1;
	}
	if (eptr->mode != KILL && srceptr->mode != KILL) {
		data = matocsserv_createpacket(eptr, MATOCS_REPLICATE, 8+4+4+2);
		put64bit(&data, chunkid);
		put32bit(&data, version);
		put32bit(&data, srceptr->servip);
		put16bit(&data, srceptr->servport);
		matocsserv_replication_begin(chunkid, version, ChunkType::getStandardChunkType(), eptr, 1,
				&srceptr);
	}
	return 0;
}

int matocsserv_send_liz_replicatechunk(matocsserventry *eptr, uint64_t chunkid, uint32_t version, ChunkType type,
		const std::vector<matocsserventry*> &sourcePointers, const std::vector<ChunkType> &sourceTypes) {
	if (matocsserv_replication_find(chunkid, version, type, eptr)) {
		return -1;
	}
	if (sourcePointers.size() != sourceTypes.size()) {
		syslog(LOG_ERR, "Inconsistent arguments for liz_replicatechunk (%u != %u)",
				static_cast<unsigned>(sourcePointers.size()),
				static_cast<unsigned>(sourceTypes.size()));
		return -1;
	}
	if (eptr->mode == KILL) {
		return 0;
	}
	for (auto source : sourcePointers) {
		if (source->mode == KILL) {
			return 0;
		}
	}
	std::vector<ChunkTypeWithAddress> sources;
	for (size_t i = 0; i < sourcePointers.size(); ++i) {
		matocsserventry *src = sourcePointers[i];
		sources.push_back(ChunkTypeWithAddress(
				NetworkAddress(src->servip, src->servport), sourceTypes[i]));
	}
	eptr->outputPackets.push_back(OutputPacket());
	matocs::replicateChunk::serialize(eptr->outputPackets.back().packet,
			chunkid, version, type, sources);
	matocsserv_replication_begin(chunkid, version, type,
			eptr, sourcePointers.size(), sourcePointers.data());
	return 0;
}

void matocsserv_got_replicatechunk_status(matocsserventry *eptr, const std::vector<uint8_t> &data,
		 uint32_t packetType) {
	uint64_t chunkId;
	uint32_t chunkVersion;
	ChunkType chunkType = ChunkType::getStandardChunkType();
	uint8_t status;

	if (packetType == LIZ_CSTOMA_REPLICATE_CHUNK) {
		cstoma::replicateChunk::deserialize(data, chunkId, chunkType, status, chunkVersion);
	} else {
		sassert(packetType == CSTOMA_REPLICATE);
		deserializeAllMooseFsPacketDataNoHeader(data, chunkId, chunkVersion, status);
	}

	matocsserv_replication_end(chunkId, chunkVersion, chunkType, eptr);
	chunk_got_replicate_status(eptr, chunkId, chunkVersion, chunkType, status);
	if (status != 0) {
		syslog(LOG_NOTICE, "(%s:%" PRIu16 ") chunk: %016" PRIX64 " replication status: %s",
				eptr->servstrip, eptr->servport, chunkId, mfsstrerr(status));
	}
}

int matocsserv_send_setchunkversion(matocsserventry *eptr, uint64_t chunkId, uint32_t newVersion,
		uint32_t chunkVersion, ChunkType chunkType) {
	if (eptr->mode != KILL) {
		eptr->outputPackets.push_back(OutputPacket());
		if (eptr->version < kFirstXorVersion) {
			// send old packet when chunkserver doesn't support xor chunks
			sassert(chunkType == ChunkType::getStandardChunkType());
			serializeMooseFsPacket(eptr->outputPackets.back().packet, MATOCS_SET_VERSION,
					chunkId, newVersion, chunkVersion);
		}
		else {
			matocs::setVersion::serialize(eptr->outputPackets.back().packet, chunkId, chunkType,
					chunkVersion, newVersion);
		}
	}
	return 0;
}

void matocsserv_got_setchunkversion_status(matocsserventry *eptr,
		const std::vector<uint8_t>& data) {
	uint64_t chunkId;
	ChunkType chunkType = ChunkType::getStandardChunkType();
	uint8_t status;

	if (eptr->version < kFirstXorVersion) {
		deserializeAllMooseFsPacketDataNoHeader(data, chunkId, status);
	} else {
		cstoma::setVersion::deserialize(data, chunkId, chunkType, status);
	}

	chunk_got_setversion_status(eptr, chunkId, chunkType, status);
	if (status != 0) {
		syslog(LOG_NOTICE, "(%s:%" PRIu16 ") chunk: %016" PRIX64 " set version status: %s",
				eptr->servstrip, eptr->servport, chunkId, mfsstrerr(status));
	}
}

int matocsserv_send_duplicatechunk(matocsserventry* eptr, uint64_t newChunkId, uint32_t newChunkVersion,
		ChunkType chunkType, uint64_t chunkId, uint32_t chunkVersion) {
	if (eptr->mode == KILL) {
		return 0;
	}

	OutputPacket outPacket;
	if (chunkType.isStandardChunkType() && eptr->version < kFirstXorVersion) {
		// Legacy support
		serializeMooseFsPacket(outPacket.packet, MATOCS_DUPLICATE, newChunkId, newChunkVersion,
				chunkId, chunkVersion);
	} else {
		matocs::duplicateChunk::serialize(outPacket.packet, newChunkId, newChunkVersion,
				chunkType, chunkId, chunkVersion);
	}
	eptr->outputPackets.push_back(std::move(outPacket));
	return 0;
}

void matocsserv_got_duplicatechunk_status(matocsserventry* eptr, const std::vector<uint8_t>& data) {
	uint64_t chunkId;
	ChunkType chunkType = ChunkType::getStandardChunkType();
	uint8_t status;
	if (eptr->version < kFirstXorVersion) {
		deserializeAllMooseFsPacketDataNoHeader(data, chunkId, status);
	} else {
		cstoma::duplicateChunk::deserialize(data, chunkId, chunkType, status);
	}

	chunk_got_duplicate_status(eptr, chunkId, chunkType, status);
	if (status != 0) {
		syslog(LOG_NOTICE, "(%s:%" PRIu16 ") chunk: %016" PRIX64 ", type: %" PRIu8
				" duplication status: %s", eptr->servstrip, eptr->servport,
				chunkId, chunkType.chunkTypeId(), mfsstrerr(status));
	}
}

void matocsserv_send_truncatechunk(matocsserventry* eptr, uint64_t chunkid, ChunkType chunkType, uint32_t length,
		uint32_t newVersion,uint32_t oldVersion) {
	uint8_t *data;

	if (eptr->mode == KILL) {
		return;
	}
	if (chunkType.isStandardChunkType() && eptr->version < kFirstXorVersion) {
		// For MooseFS 1.6.27
		data = matocsserv_createpacket(eptr,MATOCS_TRUNCATE,8+4+4+4);
		put64bit(&data,chunkid);
		put32bit(&data,length);
		put32bit(&data,newVersion);
		put32bit(&data,oldVersion);
	} else {
		eptr->outputPackets.push_back(OutputPacket());
		matocs::truncateChunk::serialize(eptr->outputPackets.back().packet,
				chunkid, chunkType, length, newVersion, oldVersion);
	}
}

void matocsserv_got_truncatechunk_status(matocsserventry *eptr, const uint8_t *data,
		uint32_t length) {
	uint64_t chunkid;
	uint8_t status;
	if (length!=8+1) {
		syslog(LOG_NOTICE,"CSTOMA_TRUNCATE - wrong size (%" PRIu32 "/9)",length);
		eptr->mode=KILL;
		return;
	}
	passert(data);
	chunkid = get64bit(&data);
	status = get8bit(&data);
	chunk_got_truncate_status(eptr, chunkid, ChunkType::getStandardChunkType(), status);
	if (status!=0) {
		syslog(LOG_NOTICE,"(%s:%" PRIu16 ") chunk: %016" PRIX64 " truncate status: %s",eptr->servstrip,eptr->servport,chunkid,mfsstrerr(status));
	}
}

void matocsserv_got_liz_truncatechunk_status(matocsserventry *eptr,
		const std::vector<uint8_t>& data) {
	uint64_t chunkId;
	ChunkType chunkType = ChunkType::getStandardChunkType();
	uint8_t status;
	cstoma::truncate::deserialize(data, chunkId, chunkType, status);

	chunk_got_truncate_status(eptr, chunkId, chunkType, status);
	if (status!=0) {
		syslog(LOG_NOTICE,"(%s:%" PRIu16 ") chunk: %016" PRIX64 ", type: %08" PRIX32
				" truncate status: %s", eptr->servstrip, eptr->servport, chunkId,
				chunkType.chunkTypeId(), mfsstrerr(status));
	}
}

int matocsserv_send_duptruncchunk(matocsserventry* eptr, uint64_t newChunkId, uint32_t newChunkVersion,
		ChunkType chunkType, uint64_t chunkId, uint32_t chunkVersion, uint32_t newChunkLength) {
	if (eptr->mode == KILL) {
		return 0;
	}

	OutputPacket outPacket;
	if (chunkType.isStandardChunkType() && eptr->version < kFirstXorVersion) {
		// Legacy support
		serializeMooseFsPacket(outPacket.packet, MATOCS_DUPTRUNC, newChunkId, newChunkVersion,
				chunkId, chunkVersion, newChunkLength);
	} else {
		matocs::duptruncChunk::serialize(outPacket.packet, newChunkId,
				newChunkVersion, chunkType, chunkId, chunkVersion, newChunkLength);
	}
	eptr->outputPackets.push_back(std::move(outPacket));
	return 0;
}

void matocsserv_got_duptruncchunk_status(matocsserventry* eptr, const std::vector<uint8_t>& data) {
	uint64_t chunkId;
	ChunkType chunkType = ChunkType::getStandardChunkType();
	uint8_t status;
	if (eptr->version < kFirstXorVersion) {
		deserializeAllMooseFsPacketDataNoHeader(data, chunkId, status);
	} else {
		cstoma::duptruncChunk::deserialize(data, chunkId, chunkType, status);
	}

	chunk_got_duptrunc_status(eptr, chunkId, chunkType, status);
	if (status != 0) {
		syslog(LOG_NOTICE, "(%s:%" PRIu16 ") chunk: %016" PRIX64 ", type: %" PRIu8
				" duplication with truncate status: %s", eptr->servstrip, eptr->servport,
				chunkId, chunkType.chunkTypeId(), mfsstrerr(status));
	}
}

int matocsserv_send_chunkop(matocsserventry *eptr,uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion,uint32_t leng) {
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,MATOCS_CHUNKOP,8+4+4+8+4+4);
		put64bit(&data,chunkid);
		put32bit(&data,version);
		put32bit(&data,newversion);
		put64bit(&data,copychunkid);
		put32bit(&data,copyversion);
		put32bit(&data,leng);
	}
	return 0;
}

void matocsserv_got_chunkop_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid,copychunkid;
	uint32_t version,newversion,copyversion,leng;
	uint8_t status;
	if (length!=8+4+4+8+4+4+1) {
		syslog(LOG_NOTICE,"CSTOMA_CHUNKOP - wrong size (%" PRIu32 "/33)",length);
		eptr->mode=KILL;
		return;
	}
	passert(data);
	chunkid = get64bit(&data);
	version = get32bit(&data);
	newversion = get32bit(&data);
	copychunkid = get64bit(&data);
	copyversion = get32bit(&data);
	leng = get32bit(&data);
	status = get8bit(&data);
	if (newversion!=version) {
		chunk_got_chunkop_status(eptr,chunkid,status);
	}
	if (copychunkid>0) {
		chunk_got_chunkop_status(eptr,copychunkid,status);
	}
	if (status!=0) {
		syslog(LOG_NOTICE,"(%s:%" PRIu16 ") chunkop(%016" PRIX64 ",%08" PRIX32 ",%08" PRIX32 ",%016" PRIX64 ",%08" PRIX32 ",%" PRIu32 ") status: %s",eptr->servstrip,eptr->servport,chunkid,version,newversion,copychunkid,copyversion,leng,mfsstrerr(status));
	}
}

void matocsserv_register_host(matocsserventry *eptr, uint32_t version, uint32_t servip,
		uint16_t servport, uint32_t timeout) {
	eptr->version  = version;
	eptr->servip   = servip;
	eptr->servport = servport;
	eptr->timeout  = timeout;
	if (eptr->timeout<10) {
		syslog(LOG_NOTICE, "CSTOMA_REGISTER communication timeout too small (%"
				PRIu32 " milliseconds - should be at least 10 milliseconds)", eptr->timeout);
		eptr->mode=KILL;
		return;
	}
	if (eptr->servip==0) {
		tcpgetpeer(eptr->sock,&(eptr->servip),NULL);
	}
	if (eptr->servstrip) {
		free(eptr->servstrip);
	}
	eptr->servstrip = matocsserv_makestrip(eptr->servip);
	if (((eptr->servip)&0xFF000000) == 0x7F000000) {
		syslog(LOG_NOTICE, "chunkserver connected using localhost (IP: %s) - you can't use"
				" localhost for communication between chunkserver and master", eptr->servstrip);
		eptr->mode=KILL;
		return;
	}
	if (matocsserv_csdb_new_connection(eptr->servip,eptr->servport,eptr)<0) {
		syslog(LOG_WARNING,"chunk-server already connected !!!");
		eptr->mode=KILL;
		return;
	}
	eptr->incsdb = 1;
	syslog(LOG_NOTICE, "chunkserver register begin (packet version: 5) - ip: %s, port: %"
			PRIu16, eptr->servstrip, eptr->servport);
	return;
}

void register_space(matocsserventry* eptr) {
	double us = (double)(eptr->usedspace)/(double)(1024*1024*1024);
	double ts = (double)(eptr->totalspace)/(double)(1024*1024*1024);
	syslog(LOG_NOTICE, "chunkserver register end (packet version: 5) - ip: %s, port: %"
			PRIu16 ", usedspace: %" PRIu64 " (%.2f GiB), totalspace: %" PRIu64 " (%.2f GiB)",
			eptr->servstrip, eptr->servport, eptr->usedspace, us, eptr->totalspace, ts);
}

void matocsserv_register(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t chunkversion;
	uint32_t i,chunkcount;
	uint8_t rversion;
	double us,ts;

	if (eptr->totalspace>0) {
		syslog(LOG_WARNING,"got register message from registered chunk-server !!!");
		eptr->mode=KILL;
		return;
	}

	if ((length&1)==0) {
		if (length<22 || ((length-22)%12)!=0) {
			syslog(LOG_NOTICE,"CSTOMA_REGISTER (old ver.) - wrong size (%" PRIu32 "/22+N*12)",length);
			eptr->mode=KILL;
			return;
		}
		passert(data);
		eptr->servip = get32bit(&data);
		eptr->servport = get16bit(&data);
		eptr->usedspace = get64bit(&data);
		eptr->totalspace = get64bit(&data);
		length-=22;
		rversion=0;
	} else {
		passert(data);
		rversion = get8bit(&data);
		if (rversion<=4) {
			syslog(LOG_NOTICE,"register packet version: %u",rversion);
		}
		if (rversion==1) {
			if (length<39 || ((length-39)%12)!=0) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 1) - wrong size (%" PRIu32 "/39+N*12)",length);
				eptr->mode=KILL;
				return;
			}
			eptr->servip = get32bit(&data);
			eptr->servport = get16bit(&data);
			eptr->usedspace = get64bit(&data);
			eptr->totalspace = get64bit(&data);
			eptr->todelusedspace = get64bit(&data);
			eptr->todeltotalspace = get64bit(&data);
			length-=39;
		} else if (rversion==2) {
			if (length<47 || ((length-47)%12)!=0) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 2) - wrong size (%" PRIu32 "/47+N*12)",length);
				eptr->mode=KILL;
				return;
			}
			eptr->servip = get32bit(&data);
			eptr->servport = get16bit(&data);
			eptr->usedspace = get64bit(&data);
			eptr->totalspace = get64bit(&data);
			eptr->chunkscount = get32bit(&data);
			eptr->todelusedspace = get64bit(&data);
			eptr->todeltotalspace = get64bit(&data);
			eptr->todelchunkscount = get32bit(&data);
			length-=47;
		} else if (rversion==3) {
			if (length<49 || ((length-49)%12)!=0) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 3) - wrong size (%" PRIu32 "/49+N*12)",length);
				eptr->mode=KILL;
				return;
			}
			eptr->servip = get32bit(&data);
			eptr->servport = get16bit(&data);
			eptr->timeout = 1000 * get16bit(&data);
			eptr->usedspace = get64bit(&data);
			eptr->totalspace = get64bit(&data);
			eptr->chunkscount = get32bit(&data);
			eptr->todelusedspace = get64bit(&data);
			eptr->todeltotalspace = get64bit(&data);
			eptr->todelchunkscount = get32bit(&data);
			length-=49;
		} else if (rversion==4) {
			if (length<53 || ((length-53)%12)!=0) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 4) - wrong size (%" PRIu32 "/53+N*12)",length);
				eptr->mode=KILL;
				return;
			}
			eptr->version = get32bit(&data);
			eptr->servip = get32bit(&data);
			eptr->servport = get16bit(&data);
			eptr->timeout = 1000 * get16bit(&data);
			eptr->usedspace = get64bit(&data);
			eptr->totalspace = get64bit(&data);
			eptr->chunkscount = get32bit(&data);
			eptr->todelusedspace = get64bit(&data);
			eptr->todeltotalspace = get64bit(&data);
			eptr->todelchunkscount = get32bit(&data);
			length-=53;
		} else if (rversion==50) {
			if (length!=13) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 5:BEGIN) - wrong size (%" PRIu32 "/13)",length);
				eptr->mode=KILL;
				return;
			}
			uint32_t version = get32bit(&data);
			uint32_t servip = get32bit(&data);
			uint16_t servport = get16bit(&data);
			uint32_t timeout = 1000 * get16bit(&data);
			return matocsserv_register_host(eptr, version, servip, servport, timeout);
		} else if (rversion==51) {
			if (((length-1)%12)!=0) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 5:CHUNKS) - wrong size (%" PRIu32 "/1+N*12)",length);
				eptr->mode=KILL;
				return;
			}
			chunkcount = (length-1)/12;
			for (i=0 ; i<chunkcount ; i++) {
				chunkid = get64bit(&data);
				chunkversion = get32bit(&data);
				chunk_server_has_chunk(eptr, chunkid, chunkversion,
						ChunkType::getStandardChunkType());
			}
			return;
		} else if (rversion==52) {
			if (length!=41) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 5:END) - wrong size (%" PRIu32 "/41)",length);
				eptr->mode=KILL;
				return;
			}
			eptr->usedspace = get64bit(&data);
			eptr->totalspace = get64bit(&data);
			eptr->chunkscount = get32bit(&data);
			eptr->todelusedspace = get64bit(&data);
			eptr->todeltotalspace = get64bit(&data);
			eptr->todelchunkscount = get32bit(&data);
			return register_space(eptr);
		} else {
			syslog(LOG_NOTICE,"CSTOMA_REGISTER - wrong version (%" PRIu8 "/1..4)",rversion);
			eptr->mode=KILL;
			return;
		}
	}
	if (rversion<=4) {
		if (eptr->timeout<1000) {
			syslog(LOG_NOTICE,"CSTOMA_REGISTER communication timeout too small (%" PRIu16 " milliseconds - should be at least 1 second)",eptr->timeout);
			eptr->timeout = 1000;
			return;
		}
		if (eptr->servip==0) {
			tcpgetpeer(eptr->sock,&(eptr->servip),NULL);
		}
		if (eptr->servstrip) {
			free(eptr->servstrip);
		}
		eptr->servstrip = matocsserv_makestrip(eptr->servip);
		if (((eptr->servip)&0xFF000000) == 0x7F000000) {
			syslog(LOG_NOTICE,"chunkserver connected using localhost (IP: %s) - you can't use localhost for communication between chunkserver and master", eptr->servstrip);
			eptr->mode=KILL;
			return;
		}
		us = (double)(eptr->usedspace)/(double)(1024*1024*1024);
		ts = (double)(eptr->totalspace)/(double)(1024*1024*1024);
		syslog(LOG_NOTICE,"chunkserver register - ip: %s, port: %" PRIu16 ", usedspace: %" PRIu64 " (%.2f GiB), totalspace: %" PRIu64 " (%.2f GiB)",eptr->servstrip,eptr->servport,eptr->usedspace,us,eptr->totalspace,ts);
		if (matocsserv_csdb_new_connection(eptr->servip,eptr->servport,eptr)<0) {
			syslog(LOG_WARNING,"chunk-server already connected !!!");
			eptr->mode=KILL;
			return;
		}
		eptr->incsdb = 1;
		chunkcount = length/(8+4);
		for (i=0 ; i<chunkcount ; i++) {
			chunkid = get64bit(&data);
			chunkversion = get32bit(&data);
			chunk_server_has_chunk(eptr, chunkid, chunkversion, ChunkType::getStandardChunkType());
		}
	}
}

void matocsserv_space(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	if (length!=16 && length!=32 && length!=40) {
		syslog(LOG_NOTICE,"CSTOMA_SPACE - wrong size (%" PRIu32 "/16|32|40)",length);
		eptr->mode=KILL;
		return;
	}
	passert(data);
	eptr->usedspace = get64bit(&data);
	eptr->totalspace = get64bit(&data);
	if (length==40) {
		eptr->chunkscount = get32bit(&data);
	}
	if (length>=32) {
		eptr->todelusedspace = get64bit(&data);
		eptr->todeltotalspace = get64bit(&data);
		if (length==40) {
			eptr->todelchunkscount = get32bit(&data);
		}
	}
}

void matocsserv_liz_register_host(matocsserventry *eptr, const std::vector<uint8_t>& data)
		throw (IncorrectDeserializationException) {
	uint32_t version;
	uint32_t servip;
	uint16_t servport;
	uint32_t timeout;
	cstoma::registerHost::deserialize(data, servip, servport, timeout, version);
	return matocsserv_register_host(eptr, version, servip, servport, timeout);
}

void matocsserv_liz_register_chunks(matocsserventry *eptr, const std::vector<uint8_t>& data)
		throw (IncorrectDeserializationException) {
	PacketVersion v;
	deserializePacketVersionNoHeader(data, v);
	if (v == cstoma::registerChunks::kStandardAndXorChunks) {
		std::vector<ChunkWithVersionAndType> chunks;
		cstoma::registerChunks::deserialize(data, chunks);
		for (auto& chunk : chunks) {
			chunk_server_has_chunk(eptr, chunk.id, chunk.version, chunk.type);
		}
	} else {
		std::vector<ChunkWithVersion> chunks;
		cstoma::registerChunks::deserialize(data, chunks);
		for (auto& chunk : chunks) {
			chunk_server_has_chunk(eptr, chunk.id, chunk.version, ChunkType::getStandardChunkType());
		}
	}
}

void matocsserv_liz_register_space(matocsserventry *eptr, const std::vector<uint8_t>& data)
		throw (IncorrectDeserializationException) {
	cstoma::registerSpace::deserialize(data, eptr->usedspace, eptr->totalspace, eptr->chunkscount,
			eptr->todelusedspace, eptr->todeltotalspace, eptr->todelchunkscount);
	return register_space(eptr);
}

void matocsserv_liz_register_label(matocsserventry *eptr, const std::vector<uint8_t>& data)
		throw (IncorrectDeserializationException) {
	std::string label;
	cstoma::registerLabel::deserialize(data, label);
	if (!isMediaLabelValid(label)) {
		syslog(LOG_NOTICE,"LIZ_CSTOMA_REGISTER_LABEL - wrong label '%s' of chunkserver "
				"(ip: %s, port %" PRIu16 ")", label.c_str(), eptr->servstrip, eptr->servport);
		eptr->mode = KILL;
		return;
	}
	if (label != eptr->label) {
		syslog(LOG_NOTICE, "chunkserver (ip: %s, port %" PRIu16 ") "
				"changed its label from '%s' to '%s'",
				eptr->servstrip, eptr->servport, eptr->label.c_str(), label.c_str());
		chunk_server_label_changed(eptr->label, label);
		eptr->label = std::move(label);
	}
}

void matocsserv_chunk_damaged(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t i;

	if (length%8!=0) {
		syslog(LOG_NOTICE,"CSTOMA_CHUNK_DAMAGED - wrong size (%" PRIu32 "/N*8)",length);
		eptr->mode=KILL;
		return;
	}
	if (length>0) {
		passert(data);
	}
	for (i=0 ; i<length/8 ; i++) {
		chunkid = get64bit(&data);
//              syslog(LOG_NOTICE,"(%s:%" PRIu16 ") chunk: %016" PRIX64 " is damaged",eptr->servstrip,eptr->servport,chunkid);
		chunk_damaged(eptr,chunkid);
	}
}

void matocsserv_chunks_lost(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t i;

	if (length%8!=0) {
		syslog(LOG_NOTICE,"CSTOMA_CHUNK_LOST - wrong size (%" PRIu32 "/N*8)",length);
		eptr->mode=KILL;
		return;
	}
	if (length>0) {
		passert(data);
	}
	for (i=0 ; i<length/8 ; i++) {
		chunkid = get64bit(&data);
//              syslog(LOG_NOTICE,"(%s:%" PRIu16 ") chunk lost: %016" PRIX64,eptr->servstrip,eptr->servport,chunkid);
		chunk_lost(eptr,chunkid);
	}
}

void matocsserv_chunks_new(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t chunkversion;
	uint32_t i;

	if (length%12!=0) {
		syslog(LOG_NOTICE,"CSTOMA_CHUNK_NEW - wrong size (%" PRIu32 "/N*12)",length);
		eptr->mode=KILL;
		return;
	}
	if (length>0) {
		passert(data);
	}
	for (i=0 ; i<length/12 ; i++) {
		chunkid = get64bit(&data);
		chunkversion = get32bit(&data);
//              syslog(LOG_NOTICE,"(%s:%" PRIu16 ") chunk lost: %016" PRIX64,eptr->servstrip,eptr->servport,chunkid);
		chunk_server_has_chunk(eptr, chunkid, chunkversion, ChunkType::getStandardChunkType());
	}
}

void matocsserv_liz_chunk_new(matocsserventry *eptr, const std::vector<uint8_t>& data) {
	std::vector<ChunkWithVersionAndType> chunks;
	cstoma::chunkNew::deserialize(data, chunks);
	for (auto& chunk : chunks) {
		chunk_server_has_chunk(eptr, chunk.id, chunk.version, chunk.type);
	}
}

void matocsserv_error_occurred(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CSTOMA_ERROR_OCCURRED - wrong size (%" PRIu32 "/0)",length);
		eptr->mode=KILL;
		return;
	}
	eptr->errorcounter++;
}

void matocsserv_gotpacket(matocsserventry *eptr, PacketHeader header, const MessageBuffer& data) {
	uint32_t length = data.size();
	try {
		switch (header.type) {
			case ANTOAN_NOP:
				break;
			case ANTOAN_UNKNOWN_COMMAND: // for future use
				break;
			case ANTOAN_BAD_COMMAND_SIZE: // for future use
				break;
			case CSTOMA_REGISTER:
				matocsserv_register(eptr, data.data(), length);
				break;
			case CSTOMA_SPACE:
				matocsserv_space(eptr, data.data(), length);
				break;
			case CSTOMA_CHUNK_DAMAGED:
				matocsserv_chunk_damaged(eptr, data.data(), length);
				break;
			case CSTOMA_CHUNK_LOST:
				matocsserv_chunks_lost(eptr, data.data(), length);
				break;
			case CSTOMA_CHUNK_NEW:
				matocsserv_chunks_new(eptr, data.data(), length);
				break;
			case CSTOMA_ERROR_OCCURRED:
				matocsserv_error_occurred(eptr, data.data(), length);
				break;
			case CSTOAN_CHUNK_CHECKSUM:
				matocsserv_got_chunk_checksum(eptr, data.data(), length);
				break;
			case CSTOMA_CREATE:
			case LIZ_CSTOMA_CREATE_CHUNK:
				matocsserv_got_createchunk_status(eptr, data);
				break;
			case CSTOMA_DELETE:
			case LIZ_CSTOMA_DELETE_CHUNK:
				matocsserv_got_deletechunk_status(eptr, data);
				break;
			case CSTOMA_REPLICATE:
			case LIZ_CSTOMA_REPLICATE_CHUNK:
				matocsserv_got_replicatechunk_status(eptr, data, header.type);
				break;
			case CSTOMA_DUPLICATE:
			case LIZ_CSTOMA_DUPLICATE_CHUNK:
				matocsserv_got_duplicatechunk_status(eptr, data);
				break;
			case CSTOMA_SET_VERSION:
			case LIZ_CSTOMA_SET_VERSION:
				matocsserv_got_setchunkversion_status(eptr, data);
				break;
			case CSTOMA_TRUNCATE:
				matocsserv_got_truncatechunk_status(eptr, data.data(), length);
				break;
			case LIZ_CSTOMA_TRUNCATE:
				matocsserv_got_liz_truncatechunk_status(eptr, data);
				break;
			case CSTOMA_DUPTRUNC:
			case LIZ_CSTOMA_DUPTRUNC_CHUNK:
				matocsserv_got_duptruncchunk_status(eptr, data);
				break;
			case LIZ_CSTOMA_CHUNK_NEW:
				matocsserv_liz_chunk_new(eptr, data);
				break;
			case LIZ_CSTOMA_REGISTER_HOST:
				matocsserv_liz_register_host(eptr, data);
				break;
			case LIZ_CSTOMA_REGISTER_CHUNKS:
				matocsserv_liz_register_chunks(eptr, data);
				break;
			case LIZ_CSTOMA_REGISTER_SPACE:
				matocsserv_liz_register_space(eptr, data);
				break;
			case LIZ_CSTOMA_REGISTER_LABEL:
				matocsserv_liz_register_label(eptr, data);
				break;
			default:
				syslog(LOG_NOTICE,"master <-> chunkservers module: got unknown message "
						"(type:%" PRIu32 ")", header.type);
				eptr->mode=KILL;
				break;
		}
	} catch (IncorrectDeserializationException& e) {
		syslog(LOG_NOTICE,
				"master <-> chunkservers module: got inconsistent message "
				"(type:%" PRIu32 ", length:%" PRIu32"), %s", header.type, length, e.what());
		eptr->mode = KILL;
	}
}

void matocsserv_term(void) {
	matocsserventry *eptr,*eaptr;
	syslog(LOG_INFO,"master <-> chunkservers module: closing %s:%s",ListenHost,ListenPort);
	tcpclose(lsock);

	eptr = matocsservhead;
	while (eptr) {
		if (eptr->servstrip) {
			free(eptr->servstrip);
		}
		eaptr = eptr;
		eptr = eptr->next;
		delete eaptr;
	}
	matocsservhead=NULL;

	free(ListenHost);
	free(ListenPort);
}

void matocsserv_read(matocsserventry *eptr) {
	for (;;) {
		uint32_t bytesToRead = eptr->inputPacket.bytesToBeRead();
		ssize_t ret = read(eptr->sock, eptr->inputPacket.pointerToBeReadInto(), bytesToRead);
		if (ret == 0) {
			lzfs_pretty_syslog(LOG_NOTICE, "connection with CS(%s) has been closed by peer",
					eptr->servstrip);
			eptr->mode = KILL;
			return;
		} else if (ret < 0) {
			if (errno != EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE, "read from CS(%s) error", eptr->servstrip);
				eptr->mode = KILL;
			}
			return;
		}

		try {
			eptr->inputPacket.increaseBytesRead(ret);
		} catch (InputPacketTooLongException& ex) {
			lzfs_pretty_syslog(LOG_WARNING, "reading from CS(%s): %s", eptr->servstrip, ex.what());
			eptr->mode = KILL;
			return;
		}
		if (ret == bytesToRead && !eptr->inputPacket.hasData()) {
			// there might be more data to read in socket's buffer
			continue;
		} else if (!eptr->inputPacket.hasData()) {
			return;
		}

		matocsserv_gotpacket(eptr, eptr->inputPacket.getHeader(), eptr->inputPacket.getData());
		eptr->inputPacket.reset();
	}
}

void matocsserv_write(matocsserventry *eptr) {
	int32_t i;
	while (!eptr->outputPackets.empty()) {
		OutputPacket& pack = eptr->outputPackets.front();
		i = write(eptr->sock, pack.packet.data() + pack.bytesSent,
				pack.packet.size() - pack.bytesSent);
		if (i<0) {
			if (errno!=EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE,"write to CS(%s) error",eptr->servstrip);
				eptr->mode = KILL;
			}
			return;
		}
		pack.bytesSent += i;
		if (pack.packet.size() != pack.bytesSent) {
			return;
		}
		eptr->outputPackets.pop_front();
	}
}

void matocsserv_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	matocsserventry *eptr;
	pdesc[pos].fd = lsock;
	pdesc[pos].events = POLLIN;
	lsockpdescpos = pos;
	pos++;
	for (eptr=matocsservhead ; eptr ; eptr=eptr->next) {
		pdesc[pos].fd = eptr->sock;
		pdesc[pos].events = POLLIN;
		eptr->pdescpos = pos;
		if (!eptr->outputPackets.empty()) {
			pdesc[pos].events |= POLLOUT;
		}
		pos++;
	}
	*ndesc = pos;
}

void matocsserv_serve(struct pollfd *pdesc) {
	uint32_t peerip;
	matocsserventry *eptr,**kptr;
	int ns;

	if (lsockpdescpos>=0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
		ns=tcpaccept(lsock);
		if (ns<0) {
			lzfs_silent_errlog(LOG_NOTICE,"master<->CS socket: accept error");
		} else if (metadataserver::isMaster()) {
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = new matocsserventry;
			passert(eptr);
			eptr->next = matocsservhead;
			matocsservhead = eptr;
			eptr->sock = ns;
			eptr->pdescpos = -1;
			eptr->mode = CONNECTED;
			eptr->lastread.reset();
			eptr->lastwrite.reset();

			tcpgetpeer(eptr->sock,&peerip,NULL);
			eptr->servstrip = matocsserv_makestrip(peerip);
			eptr->version = 0;
			eptr->servip = 0;
			eptr->servport = 0;
			eptr->timeout = 60000;
			eptr->label = kMediaLabelWildcard;
			eptr->usedspace = 0;
			eptr->totalspace = 0;
			eptr->chunkscount = 0;
			eptr->todelusedspace = 0;
			eptr->todeltotalspace = 0;
			eptr->todelchunkscount = 0;
			eptr->errorcounter = 0;
			eptr->rrepcounter = 0;
			eptr->wrepcounter = 0;
			eptr->delcounter = 0;
			eptr->incsdb = 0;
			chunk_server_unlabelled_connected();
		} else {
			tcpclose(ns);
		}
	}
	for (eptr=matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP)) {
				eptr->mode = KILL;
			}
			if ((pdesc[eptr->pdescpos].revents & POLLIN) && eptr->mode!=KILL) {
				eptr->lastread.reset();
				matocsserv_read(eptr);
			}
			if ((pdesc[eptr->pdescpos].revents & POLLOUT) && eptr->mode!=KILL) {
				eptr->lastwrite.reset();
				matocsserv_write(eptr);
			}
		}
		if (eptr->lastread.elapsed_ms() > eptr->timeout) {
			eptr->mode = KILL;
		}
		if (eptr->lastwrite.elapsed_ms() > (eptr->timeout/3) && eptr->outputPackets.empty()) {
			matocsserv_createpacket(eptr,ANTOAN_NOP,0);
		}
	}
	kptr = &matocsservhead;
	while ((eptr=*kptr)) {
		if (eptr->mode == KILL) {
			double us,ts;
			us = (double)(eptr->usedspace)/(double)(1024*1024*1024);
			ts = (double)(eptr->totalspace)/(double)(1024*1024*1024);
			syslog(LOG_NOTICE,
					"chunkserver disconnected - ip: %s, port: %" PRIu16
					", usedspace: %" PRIu64 " (%.2f GiB), totalspace: %" PRIu64
					" (%.2f GiB)", eptr->servstrip, eptr->servport, eptr->usedspace,
					us, eptr->totalspace, ts);
			matocsserv_replication_disconnected(eptr);
			chunk_server_disconnected(eptr, eptr->label);
			if (eptr->incsdb) {
				matocsserv_csdb_lost_connection(eptr->servip,eptr->servport);
			}
			tcpclose(eptr->sock);

			if (eptr->servstrip) {
				free(eptr->servstrip);
			}
			*kptr = eptr->next;
		} else {
			kptr = &(eptr->next);
		}
	}
}

void matocsserv_remove_server(matocsserventry *ptr) {
	delete ptr;
}

void matocsserv_reload(void) {
	char *oldListenHost,*oldListenPort;
	int newlsock;

	oldListenHost = ListenHost;
	oldListenPort = ListenPort;
	ListenHost = cfg_getstr("MATOCS_LISTEN_HOST","*");
	ListenPort = cfg_getstr("MATOCS_LISTEN_PORT","9420");
	if (strcmp(oldListenHost,ListenHost)==0 && strcmp(oldListenPort,ListenPort)==0) {
		free(oldListenHost);
		free(oldListenPort);
		lzfs_pretty_syslog(LOG_NOTICE,"master <-> chunkservers module: socket address hasn't changed (%s:%s)",ListenHost,ListenPort);
		return;
	}

	newlsock = tcpsocket();
	if (newlsock<0) {
		lzfs_pretty_errlog(LOG_WARNING,"master <-> chunkservers module: socket address has changed, but can't create new socket");
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		return;
	}
	tcpnonblock(newlsock);
	tcpnodelay(newlsock);
	tcpreuseaddr(newlsock);
	if (tcpsetacceptfilter(newlsock)<0 && errno!=ENOTSUP) {
		lzfs_silent_errlog(LOG_NOTICE,"master <-> chunkservers module: can't set accept filter");
	}
	if (tcpstrlisten(newlsock,ListenHost,ListenPort,100)<0) {
		lzfs_pretty_errlog(LOG_ERR,"master <-> chunkservers module: socket address has changed, but can't listen on socket (%s:%s)",ListenHost,ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		tcpclose(newlsock);
		return;
	}
	lzfs_pretty_syslog(LOG_NOTICE,"master <-> chunkservers module: socket address has changed, now listen on %s:%s",ListenHost,ListenPort);
	free(oldListenHost);
	free(oldListenPort);
	tcpclose(lsock);
	lsock = newlsock;
}

uint32_t matocsserv_get_version(matocsserventry *e) {
	return e->version;
}

int matocsserv_init(void) {
	ListenHost = cfg_getstr("MATOCS_LISTEN_HOST","*");
	ListenPort = cfg_getstr("MATOCS_LISTEN_PORT","9420");

	lsock = tcpsocket();
	if (lsock<0) {
		lzfs_pretty_errlog(LOG_ERR,"master <-> chunkservers module: can't create socket");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		lzfs_silent_errlog(LOG_NOTICE,"master <-> chunkservers module: can't set accept filter");
	}
	if (tcpstrlisten(lsock,ListenHost,ListenPort,100)<0) {
		lzfs_pretty_errlog(LOG_ERR,"master <-> chunkservers module: can't listen on %s:%s",ListenHost,ListenPort);
		return -1;
	}
	lzfs_pretty_syslog(LOG_NOTICE,"master <-> chunkservers module: listen on %s:%s",ListenHost,ListenPort);

	matocsserv_replication_init();
	matocsserv_csdb_init();
	matocsservhead = NULL;
	main_reloadregister(matocsserv_reload);
	main_destructregister(matocsserv_term);
	main_pollregister(matocsserv_desc,matocsserv_serve);
	return 0;
}
