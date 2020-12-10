/*
   Copyright 2005-2017 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2017 Skytechnology sp. z o.o..

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
#include "master/matoclserv.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <math.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>
#include <fstream>
#include <memory>

#include "common/cfg.h"
#include "common/charts.h"
#include "common/chunk_type_with_address.h"
#include "common/chunk_with_address_and_label.h"
#include "common/chunks_availability_state.h"
#include "common/datapack.h"
#include "common/event_loop.h"
#include "common/generic_lru_cache.h"
#include "common/goal.h"
#include "common/human_readable_format.h"
#include "common/io_limits_config_loader.h"
#include "common/io_limits_database.h"
#include "common/lizardfs_statistics.h"
#include "common/lizardfs_version.h"
#include "common/loop_watchdog.h"
#include "common/massert.h"
#include "common/md5.h"
#include "common/metadata.h"
#include "common/moosefs_vector.h"
#include "common/network_address.h"
#include "common/random.h"
#include "common/serialized_goal.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "common/user_groups.h"
#include "master/changelog.h"
#include "master/chartsdata.h"
#include "master/chunks.h"
#include "master/chunkserver_db.h"
#include "master/datacachemgr.h"
#include "master/exports.h"
#include "master/filesystem.h"
#include "master/filesystem_operations.h"
#include "master/filesystem_periodic.h"
#include "master/filesystem_snapshot.h"
#include "master/masterconn.h"
#include "master/matocsserv.h"
#include "master/matomlserv.h"
#include "master/personality.h"
#include "master/settrashtime_task.h"
#include "protocol/cltoma.h"
#include "protocol/matocl.h"
#include "protocol/MFSCommunication.h"

#define MaxPacketSize 1000000

// matoclserventry.mode
enum {KILL,HEADER,DATA};
// chunklis.type
enum {
	FUSE_WRITE,          // reply to FUSE_WRITE_CHUNK is delayed
	FUSE_TRUNCATE,       // reply to FUSE_TRUNCATE which does not require writing is delayed
	FUSE_TRUNCATE_BEGIN, // reply to FUSE_TRUNCATE which does require writing is delayed
	FUSE_TRUNCATE_END    // reply to FUSE_TRUNCATE_END is delayed
};

#define SESSION_STATS 16

const uint32_t kMaxNumberOfChunkCopies = 100U;

struct matoclserventry;

// locked chunks
class PacketSerializer;
typedef struct chunklist {
	uint64_t chunkid;
	uint64_t fleng;     // file length
	uint32_t lockid;    // lock ID
	uint32_t qid;       // queryid for answer
	uint32_t inode;     // inode
	uint32_t uid;
	uint32_t gid;
	uint32_t auid;
	uint32_t agid;
	uint8_t type;
	const PacketSerializer* serializer;
	struct chunklist *next;
} chunklist;

// opened files
typedef struct filelist {
	uint32_t inode;
	struct filelist *next;
} filelist;

struct session {
	typedef GenericLruCache<uint32_t, FsContext::GroupsContainer, 1024> GroupCache;

	uint32_t sessionid;
	char *info;
	uint32_t peerip;
	uint8_t newsession;
	uint8_t sesflags;
	uint8_t mingoal;
	uint8_t maxgoal;
	uint32_t mintrashtime;
	uint32_t maxtrashtime;
	uint32_t rootuid;
	uint32_t rootgid;
	uint32_t mapalluid;
	uint32_t mapallgid;
	uint32_t rootinode;
	uint32_t disconnected;  // 0 = connected ; other = disconnection timestamp
	uint32_t nsocks;        // >0 - connected (number of active connections) ; 0 - not connected
	std::array<uint32_t,SESSION_STATS> currentopstats;
	std::array<uint32_t,SESSION_STATS> lasthouropstats;
	GroupCache group_cache;
	filelist *openedfiles;
	struct session *next;

	session()
	    : sessionid(),
	      info(),
	      peerip(),
	      newsession(),
	      sesflags(),
	      mingoal(GoalId::kMin),
	      maxgoal(GoalId::kMax),
	      mintrashtime(),
	      maxtrashtime(std::numeric_limits<uint32_t>::max()),
	      rootuid(),
	      rootgid(),
	      mapalluid(),
	      mapallgid(),
	      rootinode(SPECIAL_INODE_ROOT),
	      disconnected(),
	      nsocks(),
	      currentopstats(),
	      lasthouropstats(),
	      group_cache(),
	      openedfiles(),
	      next() {
	}
};

typedef struct packetstruct {
	struct packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t *packet;
} packetstruct;

/** This looks to be the client type. This is set in matoclserv_serve and matoclserv_fuse_register, and there are 3 possible values:
 *
 *    0: new client (default, just after TCP accept)
 *       This is referred to as "unregistered clients".
 *    1: FUSE_REGISTER_BLOB_NOACL       or (FUSE_REGISTER_BLOB_ACL and (REGISTER_NEWSESSION or REGISTER_NEWMETASESSION or REGISTER_RECONNECT))
 *       This is referred to as "mounts and new tools" or "standard, registered clients".
 *  100: FUSE_REGISTER_BLOB_TOOLS_NOACL or (FUSE_REGISTER_BLOB_ACL and REGISTER_TOOLS)
 *       This is referred to as "old mfstools".
 *  665: lizardfs-admin after successful authentication
 */
enum class ClientState {
	kUnregistered = 0,
	kRegistered = 1,
	kOldTools = 100,
	kAdmin = 665
};

/// Values for matoclserventry.adminTask
/// Lists of tasks that admins can request.
enum class AdminTask {
	kNone,
	kTerminate,  ///< Admin successfully requested termination of the server
	kReload,  ///< Admin successfully requested reloading the configuration
	kSaveMetadata,  ///< Admin successfully requested saving metadata
	kRecalculateChecksums   ///< Admin successfully requested recalculation of metadata checksum
};

/** Client entry in the server. */
struct matoclserventry {
	ClientState registered;
	uint8_t mode;                           //0 - not active, 1 - read header, 2 - read packet
	bool iolimits;
	int sock;                               //socket number
	int32_t pdescpos;
	uint32_t lastread,lastwrite;            //time of last activity
	uint32_t version;
	uint32_t peerip;
	uint8_t hdrbuff[8];
	packetstruct inputpacket;
	packetstruct *outputhead,**outputtail;

	uint8_t passwordrnd[32];
	session *sesdata;
	std::unique_ptr<LizMatoclAdminRegisterChallengeData> adminChallenge;
	AdminTask adminTask;                   // admin task requested by this client
	chunklist *chunkdelayedops;

	struct matoclserventry *next;
};

static session *sessionshead=NULL;
static matoclserventry *matoclservhead=NULL;
static int lsock;
static int32_t lsockpdescpos;
static int exiting,starting;

// from config
static char *ListenHost;
static char *ListenPort;
static uint32_t RejectOld;
static uint32_t SessionSustainTime;

static uint32_t gIoLimitsAccumulate_ms;
static double gIoLimitsRefreshTime;
static uint32_t gIoLimitsConfigId;
static std::string gIoLimitsSubsystem;
static IoLimitsDatabase gIoLimitsDatabase;

static uint32_t stats_prcvd = 0;
static uint32_t stats_psent = 0;
static uint64_t stats_brcvd = 0;
static uint64_t stats_bsent = 0;

static void getStandardChunkCopies(const std::vector<ChunkTypeWithAddress>& allCopies,
		std::vector<NetworkAddress>& standardCopies);

class PacketSerializer {
public:
	static const PacketSerializer* getSerializer(PacketHeader::Type type, uint32_t version);
	virtual ~PacketSerializer() {}

	virtual bool isLizardFsPacketSerializer() const = 0;

	virtual void serializeFuseReadChunk(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint8_t status) const = 0;
	virtual void serializeFuseReadChunk(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint64_t fileLength, uint64_t chunkId, uint32_t chunkVersion,
			const std::vector<ChunkTypeWithAddress>& chunkCopies) const = 0;
	virtual void deserializeFuseReadChunk(const std::vector<uint8_t>& packetBuffer,
			uint32_t& messageId, uint32_t& inode, uint32_t& chunkIndex) const = 0;

	virtual void serializeFuseWriteChunk(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint8_t status) const = 0;
	virtual void serializeFuseWriteChunk(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint64_t fileLength,
			uint64_t chunkId, uint32_t chunkVersion, uint32_t lockId,
			const std::vector<ChunkTypeWithAddress>& chunkCopies) const = 0;
	virtual void deserializeFuseWriteChunk(const std::vector<uint8_t>& packetBuffer,
			uint32_t& messageId, uint32_t& inode, uint32_t& chunkIndex, uint32_t& lockId) const = 0;

	virtual void serializeFuseWriteChunkEnd(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint8_t status) const = 0;
	virtual void deserializeFuseWriteChunkEnd(const std::vector<uint8_t>& packetBuffer,
			uint32_t& messageId, uint64_t& chunkId, uint32_t& lockId,
			uint32_t& inode, uint64_t& fileLength) const = 0;

	virtual void serializeFuseTruncate(std::vector<uint8_t>& packetBuffer,
			uint32_t type /* FUSE_TRUNCATE | FUSE_TRUNCATE_END*/,
			uint32_t messageId, uint8_t status) const = 0;
	virtual void serializeFuseTruncate(std::vector<uint8_t>& packetBuffer,
			uint32_t type /* FUSE_TRUNCATE | FUSE_TRUNCATE_END*/,
			uint32_t messageId, const Attributes& attributes) const = 0;
	virtual void deserializeFuseTruncate(std::vector<uint8_t>& packetBuffer,
			uint32_t& messageId, uint32_t& inode, bool& isOpened,
			uint32_t& uid, uint32_t& gid, uint64_t& length) const = 0;
};

class MooseFsPacketSerializer : public PacketSerializer {
public:
	virtual bool isLizardFsPacketSerializer() const {
		return false;
	}

	virtual void serializeFuseReadChunk(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint8_t status) const {
		serializeMooseFsPacket(packetBuffer, MATOCL_FUSE_READ_CHUNK, messageId, status);
	}

	virtual void serializeFuseReadChunk(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint64_t fileLength, uint64_t chunkId, uint32_t chunkVersion,
			const std::vector<ChunkTypeWithAddress>& chunkCopies) const {
		MooseFSVector<NetworkAddress> standardChunkCopies;
		getStandardChunkCopies(chunkCopies, standardChunkCopies);
		serializeMooseFsPacket(packetBuffer, MATOCL_FUSE_READ_CHUNK, messageId, fileLength,
				chunkId, chunkVersion, standardChunkCopies);
	}

	virtual void deserializeFuseReadChunk(const std::vector<uint8_t>& packetBuffer,
			uint32_t& messageId, uint32_t& inode, uint32_t& chunkIndex) const {
		deserializeAllMooseFsPacketDataNoHeader(packetBuffer, messageId, inode, chunkIndex);
	}

	virtual void serializeFuseWriteChunk(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint8_t status) const {
		serializeMooseFsPacket(packetBuffer, MATOCL_FUSE_WRITE_CHUNK, messageId, status);
	}

	virtual void serializeFuseWriteChunk(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint64_t fileLength,
			uint64_t chunkId, uint32_t chunkVersion, uint32_t lockId,
			const std::vector<ChunkTypeWithAddress>& chunkCopies) const {
		sassert(lockId == 1);
		MooseFSVector<NetworkAddress> standardChunkCopies;
		getStandardChunkCopies(chunkCopies, standardChunkCopies);
		serializeMooseFsPacket(packetBuffer, MATOCL_FUSE_WRITE_CHUNK, messageId, fileLength,
						chunkId, chunkVersion, standardChunkCopies);
	}

	virtual void deserializeFuseWriteChunk(const std::vector<uint8_t>& packetBuffer,
			uint32_t& messageId, uint32_t& inode, uint32_t& chunkIndex, uint32_t& lockId) const {
		deserializeAllMooseFsPacketDataNoHeader(packetBuffer, messageId, inode, chunkIndex);
		lockId = 1;
	}

	virtual void serializeFuseWriteChunkEnd(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint8_t status) const {
		serializeMooseFsPacket(packetBuffer, MATOCL_FUSE_WRITE_CHUNK_END, messageId, status);
	}

	virtual void deserializeFuseWriteChunkEnd(const std::vector<uint8_t>& packetBuffer,
			uint32_t& messageId, uint64_t& chunkId, uint32_t& lockId,
			uint32_t& inode, uint64_t& fileLength) const {
		deserializeAllMooseFsPacketDataNoHeader(packetBuffer,
				messageId, chunkId, inode, fileLength);
		lockId = 1;
	}

	virtual void serializeFuseTruncate(std::vector<uint8_t>& packetBuffer,
			uint32_t type, uint32_t messageId, uint8_t status) const {
		sassert(type == FUSE_TRUNCATE || type == FUSE_TRUNCATE_END);
		if (type == FUSE_TRUNCATE) {
			serializeMooseFsPacket(packetBuffer, MATOCL_FUSE_TRUNCATE, messageId, status);
		} else {
			// this should never happen, so do anything
			serializeMooseFsPacket(packetBuffer, MATOCL_FUSE_TRUNCATE,
					messageId, uint8_t(LIZARDFS_ERROR_ENOTSUP));
		}
	}

	virtual void serializeFuseTruncate(std::vector<uint8_t>& packetBuffer,
			uint32_t type, uint32_t messageId, const Attributes& attributes) const {
		sassert(type == FUSE_TRUNCATE || type == FUSE_TRUNCATE_END);
		if (type == FUSE_TRUNCATE) {
			serializeMooseFsPacket(packetBuffer, MATOCL_FUSE_TRUNCATE, messageId, attributes);
		} else {
			// this should never happen, so do anything
			serializeMooseFsPacket(packetBuffer, MATOCL_FUSE_TRUNCATE,
					messageId, uint8_t(LIZARDFS_ERROR_ENOTSUP));
		}

	}

	virtual void deserializeFuseTruncate(std::vector<uint8_t>& packetBuffer,
			uint32_t& messageId, uint32_t& inode, bool& isOpened,
			uint32_t& uid, uint32_t& gid, uint64_t& length) const {
		deserializeAllMooseFsPacketDataNoHeader(packetBuffer,
				messageId, inode, isOpened, uid, gid, length);

	}
};

class LizardFsPacketSerializer : public PacketSerializer {
public:
	virtual bool isLizardFsPacketSerializer() const {
		return true;
	}

	virtual void serializeFuseReadChunk(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint8_t status) const {
		matocl::fuseReadChunk::serialize(packetBuffer, messageId, status);
	}

	virtual void serializeFuseReadChunk(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint64_t fileLength, uint64_t chunkId, uint32_t chunkVersion,
			const std::vector<ChunkTypeWithAddress>& chunkCopies) const {
		matocl::fuseReadChunk::serialize(packetBuffer, messageId, fileLength, chunkId, chunkVersion,
				chunkCopies);
	}

	virtual void deserializeFuseReadChunk(const std::vector<uint8_t>& packetBuffer,
			uint32_t& messageId, uint32_t& inode, uint32_t& chunkIndex) const {
		cltoma::fuseReadChunk::deserialize(packetBuffer, messageId, inode, chunkIndex);
	}

	virtual void serializeFuseWriteChunk(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint8_t status) const {
		matocl::fuseWriteChunk::serialize(packetBuffer, messageId, status);
	}

	virtual void serializeFuseWriteChunk(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint64_t fileLength,
			uint64_t chunkId, uint32_t chunkVersion, uint32_t lockId,
			const std::vector<ChunkTypeWithAddress>& chunkCopies) const {
		matocl::fuseWriteChunk::serialize(packetBuffer, messageId,
				fileLength, chunkId, chunkVersion, lockId, chunkCopies);
	}

	virtual void deserializeFuseWriteChunk(const std::vector<uint8_t>& packetBuffer,
			uint32_t& messageId, uint32_t& inode, uint32_t& chunkIndex, uint32_t& lockId) const {
		cltoma::fuseWriteChunk::deserialize(packetBuffer, messageId, inode, chunkIndex, lockId);
	}

	virtual void serializeFuseWriteChunkEnd(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint8_t status) const {
		matocl::fuseWriteChunkEnd::serialize(packetBuffer, messageId, status);
	}

	virtual void deserializeFuseWriteChunkEnd(const std::vector<uint8_t>& packetBuffer,
			uint32_t& messageId, uint64_t& chunkId, uint32_t& lockId,
			uint32_t& inode, uint64_t& fileLength) const {
		cltoma::fuseWriteChunkEnd::deserialize(packetBuffer,
				messageId, chunkId, lockId, inode, fileLength);
	}

	virtual void serializeFuseTruncate(std::vector<uint8_t>& packetBuffer,
			uint32_t type, uint32_t messageId, uint8_t status) const {
		sassert(type == FUSE_TRUNCATE || type == FUSE_TRUNCATE_END);
		if (type == FUSE_TRUNCATE) {
			matocl::fuseTruncate::serialize(packetBuffer, messageId, status);
		} else {
			matocl::fuseTruncateEnd::serialize(packetBuffer, messageId, status);
		}
	}

	virtual void serializeFuseTruncate(std::vector<uint8_t>& packetBuffer,
			uint32_t type, uint32_t messageId, const Attributes& attributes) const {
		sassert(type == FUSE_TRUNCATE || type == FUSE_TRUNCATE_END);
		if (type == FUSE_TRUNCATE) {
			matocl::fuseTruncate::serialize(packetBuffer, messageId, attributes);
		} else {
			matocl::fuseTruncateEnd::serialize(packetBuffer, messageId, attributes);
		}

	}

	virtual void deserializeFuseTruncate(std::vector<uint8_t>& packetBuffer,
			uint32_t& messageId, uint32_t& inode, bool& isOpened,
			uint32_t& uid, uint32_t& gid, uint64_t& length) const {
		cltoma::fuseTruncate::deserialize(packetBuffer,
				messageId, inode, isOpened, uid, gid, length);
	}
};

class LizardFsStdXorPacketSerializer : public LizardFsPacketSerializer {
public:
	virtual void serializeFuseReadChunk(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint64_t fileLength, uint64_t chunkId, uint32_t chunkVersion,
			const std::vector<ChunkTypeWithAddress>& chunkCopies) const {
		std::vector<legacy::ChunkTypeWithAddress> chunk_copies;
		for (const auto &part : chunkCopies) {
			if ((int)part.chunk_type.getSliceType() >= Goal::Slice::Type::kECFirst) {
				continue;
			}
			chunk_copies.push_back(legacy::ChunkTypeWithAddress(part.address, (legacy::ChunkPartType)part.chunk_type));
		}
		matocl::fuseReadChunk::serialize(packetBuffer, messageId, fileLength, chunkId, chunkVersion,
				chunk_copies);
	}

	virtual void serializeFuseWriteChunk(std::vector<uint8_t>& packetBuffer,
			uint32_t messageId, uint64_t fileLength,
			uint64_t chunkId, uint32_t chunkVersion, uint32_t lockId,
			const std::vector<ChunkTypeWithAddress>& chunkCopies) const {
		std::vector<legacy::ChunkTypeWithAddress> chunk_copies;
		for (const auto &part : chunkCopies) {
			if ((int)part.chunk_type.getSliceType() >= Goal::Slice::Type::kECFirst) {
				continue;
			}
			chunk_copies.push_back(legacy::ChunkTypeWithAddress(part.address, (legacy::ChunkPartType)part.chunk_type));
		}
		matocl::fuseWriteChunk::serialize(packetBuffer, messageId,
				fileLength, chunkId, chunkVersion, lockId, chunk_copies);
	}
};


const PacketSerializer* PacketSerializer::getSerializer(PacketHeader::Type type, uint32_t version) {
	sassert((type >= PacketHeader::kMinLizPacketType && type <= PacketHeader::kMaxLizPacketType)
			|| type <= PacketHeader::kMaxOldPacketType);
	if (type <= PacketHeader::kMaxOldPacketType) {
		static MooseFsPacketSerializer singleton;
		return &singleton;
	} else {
		static LizardFsPacketSerializer singleton;
		if (version < kFirstECVersion) {
			static LizardFsStdXorPacketSerializer singleton_stdxor;
			return &singleton_stdxor;
		}
		return &singleton;
	}
}

void matoclserv_stats(uint64_t stats[5]) {
	stats[0] = stats_prcvd;
	stats[1] = stats_psent;
	stats[2] = stats_brcvd;
	stats[3] = stats_bsent;
	stats_prcvd = 0;
	stats_psent = 0;
	stats_brcvd = 0;
	stats_bsent = 0;
}

matoclserventry *matoclserv_find_connection(uint32_t id) {
	matoclserventry *eptr;
	for (eptr = matoclservhead; eptr; eptr = eptr->next) {
		if (eptr->sesdata && eptr->sesdata->sessionid == id) {
			return eptr;
		}
	}
	return nullptr;
}

/* new registration procedure */
session* matoclserv_new_session(uint8_t newsession,uint8_t nonewid) {
	session *asesdata = new session();
	passert(asesdata);
	if (newsession==0 && nonewid) {
		asesdata->sessionid = 0;
	} else {
		asesdata->sessionid = fs_newsessionid();
	}

	asesdata->newsession = newsession;
	asesdata->nsocks = 1;
	asesdata->next = sessionshead;
	sessionshead = asesdata;
	return asesdata;
}

session* matoclserv_find_session(uint32_t sessionid) {
	session *asesdata;
	if (sessionid==0) {
		return NULL;
	}
	for (asesdata = sessionshead ; asesdata ; asesdata=asesdata->next) {
		if (asesdata->sessionid==sessionid) {
//                      syslog(LOG_NOTICE,"found: %u ; before ; nsocks: %u ; state: %u",sessionid,asesdata->nsocks,asesdata->newsession);
			if (asesdata->newsession>=2) {
				asesdata->newsession-=2;
			}
			asesdata->nsocks++;
//                      syslog(LOG_NOTICE,"found: %u ; after ; nsocks: %u ; state: %u",sessionid,asesdata->nsocks,asesdata->newsession);
			asesdata->disconnected = 0;
			return asesdata;
		}
	}
	return NULL;
}

void matoclserv_close_session(uint32_t sessionid) {
	session *asesdata;
	if (sessionid==0) {
		return;
	}
	for (asesdata = sessionshead ; asesdata ; asesdata=asesdata->next) {
		if (asesdata->sessionid==sessionid) {
//                      syslog(LOG_NOTICE,"close: %u ; before ; nsocks: %u ; state: %u",sessionid,asesdata->nsocks,asesdata->newsession);
			if (asesdata->nsocks==1 && asesdata->newsession<2) {
				asesdata->newsession+=2;
			}
//                      syslog(LOG_NOTICE,"close: %u ; after ; nsocks: %u ; state: %u",sessionid,asesdata->nsocks,asesdata->newsession);
		}
	}
	return;
}

void matoclserv_store_sessions() {
	session *asesdata;
	uint32_t ileng;
	uint8_t fsesrecord[43+SESSION_STATS*8]; // 4+4+4+4+1+1+1+4+4+4+4+4+4+SESSION_STATS*4+SESSION_STATS*4
	uint8_t *ptr;
	int i;
	FILE *fd;

	fd = fopen(kSessionsTmpFilename, "w");
	if (fd==NULL) {
		lzfs_silent_errlog(LOG_WARNING,"can't store sessions, open error");
		return;
	}
	memcpy(fsesrecord,MFSSIGNATURE "S \001\006\004",8);
	ptr = fsesrecord+8;
	put16bit(&ptr,SESSION_STATS);
	if (fwrite(fsesrecord,10,1,fd)!=1) {
		lzfs_pretty_syslog(LOG_WARNING,"can't store sessions, fwrite error");
		fclose(fd);
		return;
	}
	for (asesdata = sessionshead ; asesdata ; asesdata=asesdata->next) {
		if (asesdata->newsession==1) {
			ptr = fsesrecord;
			if (asesdata->info) {
				ileng = strlen(asesdata->info);
			} else {
				ileng = 0;
			}
			put32bit(&ptr,asesdata->sessionid);
			put32bit(&ptr,ileng);
			put32bit(&ptr,asesdata->peerip);
			put32bit(&ptr,asesdata->rootinode);
			put8bit(&ptr,asesdata->sesflags);
			put8bit(&ptr,asesdata->mingoal);
			put8bit(&ptr,asesdata->maxgoal);
			put32bit(&ptr,asesdata->mintrashtime);
			put32bit(&ptr,asesdata->maxtrashtime);
			put32bit(&ptr,asesdata->rootuid);
			put32bit(&ptr,asesdata->rootgid);
			put32bit(&ptr,asesdata->mapalluid);
			put32bit(&ptr,asesdata->mapallgid);
			for (i=0 ; i<SESSION_STATS ; i++) {
				put32bit(&ptr,asesdata->currentopstats[i]);
			}
			for (i=0 ; i<SESSION_STATS ; i++) {
				put32bit(&ptr,asesdata->lasthouropstats[i]);
			}
			if (fwrite(fsesrecord,(43+SESSION_STATS*8),1,fd)!=1) {
				lzfs_pretty_syslog(LOG_WARNING,"can't store sessions, fwrite error");
				fclose(fd);
				return;
			}
			if (ileng>0) {
				if (fwrite(asesdata->info,ileng,1,fd)!=1) {
					lzfs_pretty_syslog(LOG_WARNING,"can't store sessions, fwrite error");
					fclose(fd);
					return;
				}
			}
		}
	}
	if (fclose(fd)!=0) {
		lzfs_silent_errlog(LOG_WARNING,"can't store sessions, fclose error");
		return;
	}
	if (rename(kSessionsTmpFilename, kSessionsFilename) < 0) {
		lzfs_silent_errlog(LOG_WARNING,"can't store sessions, rename error");
	}
}

int matoclserv_load_sessions() {
	session *asesdata;
	uint32_t ileng;
	uint8_t hdr[8];
	uint8_t *fsesrecord;
	const uint8_t *ptr;
	uint8_t mapalldata;
	uint8_t goaltrashdata;
	uint32_t i,statsinfile;
	int r;
	FILE *fd;

	fd = fopen(kSessionsFilename, "r");
	if (fd==NULL) {
		lzfs_silent_errlog(LOG_WARNING,"can't load sessions, fopen error");
		if (errno==ENOENT) {    // it's ok if file does not exist
			return 0;
		} else {
			return -1;
		}
	}
	if (fread(hdr,8,1,fd)!=1) {
		lzfs_pretty_syslog(LOG_WARNING,"can't load sessions, fread error");
		fclose(fd);
		return -1;
	}
	if (memcmp(hdr,MFSSIGNATURE "S 1.5",8)==0) {
		mapalldata = 0;
		goaltrashdata = 0;
		statsinfile = 16;
	} else if (memcmp(hdr,MFSSIGNATURE "S \001\006\001",8)==0) {
		mapalldata = 1;
		goaltrashdata = 0;
		statsinfile = 16;
	} else if (memcmp(hdr,MFSSIGNATURE "S \001\006\002",8)==0) {
		mapalldata = 1;
		goaltrashdata = 0;
		statsinfile = 21;
	} else if (memcmp(hdr,MFSSIGNATURE "S \001\006\003",8)==0) {
		mapalldata = 1;
		goaltrashdata = 0;
		if (fread(hdr,2,1,fd)!=1) {
			lzfs_pretty_syslog(LOG_WARNING,"can't load sessions, fread error");
			fclose(fd);
			return -1;
		}
		ptr = hdr;
		statsinfile = get16bit(&ptr);
	} else if (memcmp(hdr,MFSSIGNATURE "S \001\006\004",8)==0) {
		mapalldata = 1;
		goaltrashdata = 1;
		if (fread(hdr,2,1,fd)!=1) {
			lzfs_pretty_syslog(LOG_WARNING,"can't load sessions, fread error");
			fclose(fd);
			return -1;
		}
		ptr = hdr;
		statsinfile = get16bit(&ptr);
	} else {
		lzfs_pretty_syslog(LOG_WARNING,"can't load sessions, bad header");
		fclose(fd);
		return -1;
	}

	if (mapalldata==0) {
		fsesrecord = (uint8_t*) malloc(25+statsinfile*8);
	} else if (goaltrashdata==0) {
		fsesrecord = (uint8_t*) malloc(33+statsinfile*8);
	} else {
		fsesrecord = (uint8_t*) malloc(43+statsinfile*8);
	}
	passert(fsesrecord);

	while (!feof(fd)) {
		if (mapalldata==0) {
			r = fread(fsesrecord,25+statsinfile*8,1,fd);
		} else if (goaltrashdata==0) {
			r = fread(fsesrecord,33+statsinfile*8,1,fd);
		} else {
			r = fread(fsesrecord,43+statsinfile*8,1,fd);
		}
		if (r==1) {
			ptr = fsesrecord;
			asesdata = new session();
			passert(asesdata);
			asesdata->sessionid = get32bit(&ptr);
			ileng = get32bit(&ptr);
			asesdata->peerip = get32bit(&ptr);
			asesdata->rootinode = get32bit(&ptr);
			asesdata->sesflags = get8bit(&ptr);
			if (goaltrashdata) {
				asesdata->mingoal = get8bit(&ptr);
				asesdata->maxgoal = get8bit(&ptr);
				asesdata->mintrashtime = get32bit(&ptr);
				asesdata->maxtrashtime = get32bit(&ptr);
			}
			asesdata->rootuid = get32bit(&ptr);
			asesdata->rootgid = get32bit(&ptr);
			if (mapalldata) {
				asesdata->mapalluid = get32bit(&ptr);
				asesdata->mapallgid = get32bit(&ptr);
			}
			asesdata->newsession = 1;
			asesdata->disconnected = eventloop_time();
			for (i=0 ; i<SESSION_STATS ; i++) {
				asesdata->currentopstats[i] = (i<statsinfile)?get32bit(&ptr):0;
			}
			if (statsinfile>SESSION_STATS) {
				ptr+=4*(statsinfile-SESSION_STATS);
			}
			for (i=0 ; i<SESSION_STATS ; i++) {
				asesdata->lasthouropstats[i] = (i<statsinfile)?get32bit(&ptr):0;
			}
			if (ileng>0) {
				asesdata->info = (char*) malloc(ileng+1);
				passert(asesdata->info);
				if (fread(asesdata->info,ileng,1,fd)!=1) {
					free(asesdata->info);
					delete asesdata;
					free(fsesrecord);
					lzfs_pretty_syslog(LOG_WARNING,"can't load sessions, fread error");
					fclose(fd);
					return -1;
				}
				asesdata->info[ileng]=0;
			}
			asesdata->next = sessionshead;
			sessionshead = asesdata;
		}
		if (ferror(fd)) {
			free(fsesrecord);
			lzfs_pretty_syslog(LOG_WARNING,"can't load sessions, fread error");
			fclose(fd);
			return -1;
		}
	}
	free(fsesrecord);
	lzfs_pretty_syslog(LOG_NOTICE,"sessions have been loaded");
	fclose(fd);
	return 1;
}

int matoclserv_insert_openfile(session* cr,uint32_t inode) {
	filelist *ofptr,**ofpptr;
	int status;

	ofpptr = &(cr->openedfiles);
	while ((ofptr=*ofpptr)) {
		if (ofptr->inode==inode) {
			return LIZARDFS_STATUS_OK;       // file already acquired - nothing to do
		}
		if (ofptr->inode>inode) {
			break;
		}
		ofpptr = &(ofptr->next);
	}
	status = fs_acquire(FsContext::getForMaster(eventloop_time()), inode, cr->sessionid);
	if (status==LIZARDFS_STATUS_OK) {
		ofptr = (filelist*)malloc(sizeof(filelist));
		passert(ofptr);
		ofptr->inode = inode;
		ofptr->next = *ofpptr;
		*ofpptr = ofptr;
	}
	return status;
}

void matoclserv_add_open_file(uint32_t sessionid,uint32_t inode) {
	session *asesdata;
	filelist *ofptr,**ofpptr;

	for (asesdata = sessionshead ; asesdata && asesdata->sessionid!=sessionid; asesdata=asesdata->next) ;
	if (asesdata==NULL) {
		asesdata = new session();
		passert(asesdata);
		asesdata->sessionid = sessionid;
/* session created by filesystem - only for old clients (pre 1.5.13) */
		asesdata->disconnected = eventloop_time();
		asesdata->next = sessionshead;
		sessionshead = asesdata;
	}

	ofpptr = &(asesdata->openedfiles);
	while ((ofptr=*ofpptr)) {
		if (ofptr->inode==inode) {
			return;
		}
		if (ofptr->inode>inode) {
			break;
		}
		ofpptr = &(ofptr->next);
	}
	ofptr = (filelist*)malloc(sizeof(filelist));
	passert(ofptr);
	ofptr->inode = inode;
	ofptr->next = *ofpptr;
	*ofpptr = ofptr;
}

void matoclserv_remove_open_file(uint32_t sessionid, uint32_t inode) {
	session *asesdata;

	for (asesdata = sessionshead; asesdata && asesdata->sessionid != sessionid; asesdata = asesdata->next) {
	}
	if (asesdata == NULL) {
		lzfs_pretty_syslog(LOG_ERR, "sessions file is corrupted");
		return;
	}

	filelist *ofptr = NULL;
	filelist** ofpptr = &(asesdata->openedfiles);
	while ((ofptr = *ofpptr)) {
		if (ofptr->inode == inode) {
			*ofpptr = ofptr->next;
			free(ofptr);
			break;
		}
		ofpptr = &(ofptr->next);
	}
}

void matoclserv_reset_session_timeouts() {
	session *asesdata;
	uint32_t now = eventloop_time();
	for (asesdata = sessionshead ; asesdata ; asesdata=asesdata->next) {
		asesdata->disconnected = now;
	}
}

uint8_t* matoclserv_createpacket(matoclserventry *eptr,uint32_t type,uint32_t size) {
	packetstruct *outpacket;
	uint8_t *ptr;
	uint32_t psize;

	outpacket=(packetstruct*)malloc(sizeof(packetstruct));
	passert(outpacket);
	psize = size+8;
	outpacket->packet= (uint8_t*) malloc(psize);
	passert(outpacket->packet);
	outpacket->bytesleft = psize;
	ptr = outpacket->packet;
	put32bit(&ptr,type);
	put32bit(&ptr,size);
	outpacket->startptr = (uint8_t*)(outpacket->packet);
	outpacket->next = NULL;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
	return ptr;
}

void matoclserv_createpacket(matoclserventry *eptr, const MessageBuffer& buffer) {
	packetstruct *outpacket = (packetstruct*)malloc(sizeof(packetstruct));
	passert(outpacket);
	outpacket->packet = (uint8_t*) malloc(buffer.size());
	passert(outpacket->packet);
	outpacket->bytesleft = buffer.size();
	// TODO unificate output packets and remove suboptimal memory copying
	memcpy(outpacket->packet, buffer.data(), buffer.size());
	outpacket->startptr = outpacket->packet;
	outpacket->next = NULL;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
}

static inline bool matoclserv_ugid_remap_required(matoclserventry *eptr, uint32_t uid) {
	return uid == 0 || eptr->sesdata->sesflags & SESFLAG_MAPALL;
}

static inline void matoclserv_ugid_remap(matoclserventry *eptr,uint32_t *auid,uint32_t *agid) {
	if (*auid==0) {
		*auid = eptr->sesdata->rootuid;
		if (agid) {
			*agid = eptr->sesdata->rootgid;
		}
	} else if (eptr->sesdata->sesflags&SESFLAG_MAPALL) {
		*auid = eptr->sesdata->mapalluid;
		if (agid) {
			*agid = eptr->sesdata->mapallgid;
		}
	}
}

static inline uint8_t matoclserv_check_group_cache(matoclserventry *eptr, uint32_t gid) {
	if (!user_groups::isGroupCacheId(gid)) {
		return LIZARDFS_STATUS_OK;
	}

	assert(eptr && eptr->sesdata);
	auto it = eptr->sesdata->group_cache.find(user_groups::decodeGroupCacheId(gid));
	return it == eptr->sesdata->group_cache.end() ? LIZARDFS_ERROR_GROUPNOTREGISTERED : LIZARDFS_STATUS_OK;
}

/**
 * Returns FsContext with session data but without uid/gid data
 */
static inline FsContext matoclserv_get_context(matoclserventry *eptr) {
	assert(eptr && eptr->sesdata);
	return FsContext::getForMaster(eventloop_time(), eptr->sesdata->rootinode, eptr->sesdata->sesflags);
}

/**
 * Returns FsContext with session data and uid/gid data
 */
static inline FsContext matoclserv_get_context(matoclserventry *eptr, uint32_t uid, uint32_t gid) {
	assert(eptr && eptr->sesdata);

	if (user_groups::isGroupCacheId(gid)) {
		auto it = eptr->sesdata->group_cache.find(user_groups::decodeGroupCacheId(gid));
		if (it == eptr->sesdata->group_cache.end()) {
			throw std::runtime_error("Missing group data in session cache");
		}

		assert(!it->second.empty());

		if (!matoclserv_ugid_remap_required(eptr, uid)) {
			return FsContext::getForMasterWithSession(eventloop_time(), eptr->sesdata->rootinode,
			                                          eptr->sesdata->sesflags, uid, it->second,
			                                          uid, it->second[0]);
		}

		FsContext::GroupsContainer gids;
		gids.reserve(it->second.size());

		for(const auto &orig_gid : it->second) {
			uint32_t tmp_uid = uid;
			uint32_t tmp_gid = orig_gid;
			matoclserv_ugid_remap(eptr, &tmp_uid, &tmp_gid);
			gids.push_back(tmp_gid);
		}

		uint32_t auid = uid;
		matoclserv_ugid_remap(eptr, &uid, nullptr);

		return FsContext::getForMasterWithSession(eventloop_time(),
			eptr->sesdata->rootinode, eptr->sesdata->sesflags, uid, std::move(gids), auid, it->second[0]);
	}

	uint32_t auid = uid;
	uint32_t agid = gid;
	matoclserv_ugid_remap(eptr, &uid, &gid);
	return FsContext::getForMasterWithSession(eventloop_time(),
			eptr->sesdata->rootinode, eptr->sesdata->sesflags, uid, gid, auid, agid);
}

static void getStandardChunkCopies(const std::vector<ChunkTypeWithAddress>& allCopies,
		std::vector<NetworkAddress>& standardCopies) {
	sassert(standardCopies.empty());
	for (auto& chunkCopy : allCopies) {
		if (slice_traits::isStandard(chunkCopy.chunk_type)) {
			standardCopies.push_back(chunkCopy.address);
		}
	}
}

static void remove_unsupported_ec_parts(uint32_t client_version, std::vector<ChunkTypeWithAddress> &chunk_list) {
	auto it = std::remove_if(chunk_list.begin(), chunk_list.end(),
	     [client_version](const ChunkTypeWithAddress &entry) {
		return slice_traits::isEC(entry.chunk_type) &&
		       slice_traits::ec::isEC2Part(entry.chunk_type) &&
		       (client_version < kEC2Version || entry.chunkserver_version < kEC2Version);
	});
	chunk_list.erase(it, chunk_list.end());
}

uint8_t matoclserv_fuse_write_chunk_respond(matoclserventry *eptr,
		const PacketSerializer* serializer, uint64_t chunkId, uint32_t messageId,
		uint64_t fileLength, uint32_t lockId) {
	uint32_t chunkVersion;
	std::vector<ChunkTypeWithAddress> allChunkCopies;
	uint8_t status = chunk_getversionandlocations(chunkId, eptr->peerip, chunkVersion,
			kMaxNumberOfChunkCopies, allChunkCopies);

	remove_unsupported_ec_parts(eptr->version, allChunkCopies);

	// don't allow old clients to modify standard copy of a xor chunk
	if (status == LIZARDFS_STATUS_OK && !serializer->isLizardFsPacketSerializer()) {
		for (const ChunkTypeWithAddress& chunkCopy : allChunkCopies) {
			if (!slice_traits::isStandard(chunkCopy.chunk_type)) {
				status = LIZARDFS_ERROR_NOCHUNK;
				break;
			}
		}
	}

	std::vector<uint8_t> outMessage;
	if (status == LIZARDFS_STATUS_OK) {
		serializer->serializeFuseWriteChunk(outMessage, messageId, fileLength,
				chunkId, chunkVersion, lockId, allChunkCopies);
	} else {
		serializer->serializeFuseWriteChunk(outMessage, messageId, status);
	}
	matoclserv_createpacket(eptr, outMessage);
	return status;
}

void matoclserv_chunk_status(uint64_t chunkid,uint8_t status) {
	uint32_t lockid,qid,inode,uid,gid,auid,agid;
	uint64_t fleng;
	uint8_t type;
	chunklist *cl,**acl;
	matoclserventry *eptr,*eaptr;
	const PacketSerializer *serializer;

	eptr=NULL;
	lockid=0;
	qid=0;
	fleng=0;
	type=0;
	inode=0;
	uid=0;
	gid=0;
	auid=0;
	agid=0;
	serializer = nullptr;
	for (eaptr = matoclservhead ; eaptr && eptr==NULL ; eaptr=eaptr->next) {
		if (eaptr->mode!=KILL) {
			acl = &(eaptr->chunkdelayedops);
			while (*acl && eptr==NULL) {
				cl = *acl;
				if (cl->chunkid==chunkid) {
					eptr = eaptr;
					qid = cl->qid;
					fleng = cl->fleng;
					lockid = cl->lockid;
					type = cl->type;
					inode = cl->inode;
					uid = cl->uid;
					gid = cl->gid;
					auid = cl->auid;
					agid = cl->agid;
					serializer = cl->serializer;

					*acl = cl->next;
					free(cl);
				} else {
					acl = &(cl->next);
				}
			}
		}
	}

	if (!eptr) {
		lzfs_pretty_syslog(LOG_WARNING,"got chunk status, but don't want it");
		return;
	}
	if (status==LIZARDFS_STATUS_OK) {
		dcm_modify(inode,eptr->sesdata->sessionid);
	}

	std::vector<uint8_t> reply;
	FsContext context = FsContext::getForMasterWithSession(eventloop_time(), eptr->sesdata->rootinode,
	                                             eptr->sesdata->sesflags, uid, gid, auid, agid);

	switch (type) {
	case FUSE_WRITE:
		if (status != LIZARDFS_STATUS_OK) {
			serializer->serializeFuseWriteChunk(reply, qid, status);
			matoclserv_createpacket(eptr, std::move(reply));
		} else {
			status = matoclserv_fuse_write_chunk_respond(eptr, serializer,
					chunkid, qid, fleng, lockid);
		}
		if (status != LIZARDFS_STATUS_OK) {
			fs_writeend(0, 0, chunkid, 0); // ignore status - just do it.
		}
		return;
	case FUSE_TRUNCATE_BEGIN:
		if (status != LIZARDFS_STATUS_OK) {
			matocl::fuseTruncate::serialize(reply, qid, status);
		} else {
			matocl::fuseTruncate::serialize(reply, qid, fleng, lockid);
		}
		matoclserv_createpacket(eptr, std::move(reply));
		return;
	case FUSE_TRUNCATE:
	case FUSE_TRUNCATE_END:
		fs_end_setlength(chunkid);
		if (status != LIZARDFS_STATUS_OK) {
			serializer->serializeFuseTruncate(reply, type, qid, status);
		} else {
			Attributes attr;
			fs_do_setlength(context, inode, fleng, attr);
			serializer->serializeFuseTruncate(reply, type, qid, attr);
		}
		matoclserv_createpacket(eptr, std::move(reply));
		return;
	default:
		lzfs_pretty_syslog(LOG_WARNING,"got chunk status, but operation type is unknown");
	}
}

void matoclserv_cserv_list(matoclserventry *eptr, const uint8_t */*data*/, uint32_t length) {
	if (length!=0) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_CSERV_LIST - wrong size (%" PRIu32 "/0)",length);
		eptr->mode = KILL;
		return;
	}
	auto listOfChunkservers = csdb_chunkserver_list();
	uint8_t *ptr = matoclserv_createpacket(eptr, MATOCL_CSERV_LIST, 54 * listOfChunkservers.size());
	for (const auto& server : listOfChunkservers) {
		put32bit(&ptr, server.version);
		put32bit(&ptr, server.servip);
		put16bit(&ptr, server.servport);
		put64bit(&ptr, server.usedspace);
		put64bit(&ptr, server.totalspace);
		put32bit(&ptr, server.chunkscount);
		put64bit(&ptr, server.todelusedspace);
		put64bit(&ptr, server.todeltotalspace);
		put32bit(&ptr, server.todelchunkscount);
		put32bit(&ptr, server.errorcounter);
	}
}

void matoclserv_liz_cserv_list(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	MessageBuffer buffer;
	PacketVersion version;
	bool dummy;

	deserializePacketVersionNoHeader(data, length, version);
	if (version == cltoma::cservList::kStandard) {
		matocl::cservList::serialize(buffer, csdb_chunkserver_list());
	} else if (version == cltoma::cservList::kWithMessageId) {
		uint32_t message_id;
		cltoma::cservList::deserialize(data, length, message_id, dummy);
		matocl::cservList::serialize(buffer, message_id, csdb_chunkserver_list());
	} else {
		lzfs_pretty_syslog(LOG_NOTICE,"LIZ_CSERV_LIST - wrong packet version %u", version);
		eptr->mode = KILL;
		return;
	}
	matoclserv_createpacket(eptr, std::move(buffer));
}

void matoclserv_cserv_removeserv(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t ip;
	uint16_t port;
	if (length!=6) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_CSSERV_REMOVESERV - wrong size (%" PRIu32 "/6)",length);
		eptr->mode = KILL;
		return;
	}
	ip = get32bit(&data);
	port = get16bit(&data);
	csdb_remove_server(ip,port);
	matoclserv_createpacket(eptr,MATOCL_CSSERV_REMOVESERV,0);
}

void matoclserv_iolimits_status(matoclserventry* eptr, const uint8_t* data, uint32_t length) {
	uint32_t messageId;
	cltoma::iolimitsStatus::deserialize(data, length, messageId);

	MessageBuffer buffer;
	matocl::iolimitsStatus::serialize(buffer,
			messageId,
			gIoLimitsConfigId,
			gIoLimitsRefreshTime * 1000 * 1000,
			gIoLimitsAccumulate_ms,
			gIoLimitsSubsystem,
			gIoLimitsDatabase.getGroupsAndLimits());
	matoclserv_createpacket(eptr, std::move(buffer));
}

void matoclserv_metadataserver_status(matoclserventry* eptr, const uint8_t* data, uint32_t length) {
	uint32_t messageId;
	cltoma::metadataserverStatus::deserialize(data, length, messageId);

	uint64_t metadataVersion = 0;
	try {
		metadataVersion = fs_getversion();
	} catch (NoMetadataException&) {}
	uint8_t status = metadataserver::isMaster()
		? LIZ_METADATASERVER_STATUS_MASTER
		: (masterconn_is_connected()
			 ? LIZ_METADATASERVER_STATUS_SHADOW_CONNECTED
			 : LIZ_METADATASERVER_STATUS_SHADOW_DISCONNECTED);

	MessageBuffer buffer;
	matocl::metadataserverStatus::serialize(buffer,
			messageId,
			status,
			metadataVersion);
	matoclserv_createpacket(eptr, std::move(buffer));
}

void matoclserv_list_goals(matoclserventry* eptr) {
	std::vector<SerializedGoal> serialized_goals;
	const std::map<int, Goal>& goal_map = fs_get_goal_definitions();
	for (const auto& goal : goal_map) {
		serialized_goals.emplace_back(goal.first, goal.second.getName(), to_string(goal.second));
	}
	matoclserv_createpacket(eptr, matocl::listGoals::build(serialized_goals));
}

void matoclserv_chunks_health(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	bool regularChunksOnly;
	cltoma::chunksHealth::deserialize(data, length, regularChunksOnly);
	auto message = matocl::chunksHealth::build(regularChunksOnly,
			chunk_get_availability_state(),
			chunk_get_replication_state());
	matoclserv_createpacket(eptr, std::move(message));
}

void matoclserv_session_list(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	matoclserventry *eaptr;
	uint32_t size,ileng,pleng,i;
	uint8_t vmode;
	(void)data;
	if (length!=0 && length!=1) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_SESSION_LIST - wrong size (%" PRIu32 "/0)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==0) {
		vmode = 0;
	} else {
		vmode = get8bit(&data);
	}
	size = 2;
	for (eaptr = matoclservhead ; eaptr ; eaptr=eaptr->next) {
		if (eaptr->mode!=KILL && eaptr->sesdata && eaptr->registered == ClientState::kRegistered) {
			size += 37+SESSION_STATS*8+(vmode?10:0);
			if (eaptr->sesdata->info) {
				size += strlen(eaptr->sesdata->info);
			}
			if (eaptr->sesdata->rootinode==0) {
				size += 1;
			} else {
				size += fs_getdirpath_size(eaptr->sesdata->rootinode);
			}
		}
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_SESSION_LIST,size);
	put16bit(&ptr,SESSION_STATS);
	for (eaptr = matoclservhead ; eaptr ; eaptr=eaptr->next) {
		if (eaptr->mode!=KILL && eaptr->sesdata && eaptr->registered == ClientState::kRegistered) {
			put32bit(&ptr,eaptr->sesdata->sessionid);
			put32bit(&ptr,eaptr->peerip);
			put32bit(&ptr,eaptr->version);
			if (eaptr->sesdata->info) {
				ileng = strlen(eaptr->sesdata->info);
				put32bit(&ptr,ileng);
				memcpy(ptr,eaptr->sesdata->info,ileng);
				ptr+=ileng;
			} else {
				put32bit(&ptr,0);
			}
			if (eaptr->sesdata->rootinode==0) {
				put32bit(&ptr,1);
				put8bit(&ptr,'.');
			} else {
				pleng = fs_getdirpath_size(eaptr->sesdata->rootinode);
				put32bit(&ptr,pleng);
				if (pleng>0) {
					fs_getdirpath_data(eaptr->sesdata->rootinode,ptr,pleng);
					ptr+=pleng;
				}
			}
			put8bit(&ptr,eaptr->sesdata->sesflags);
			put32bit(&ptr,eaptr->sesdata->rootuid);
			put32bit(&ptr,eaptr->sesdata->rootgid);
			put32bit(&ptr,eaptr->sesdata->mapalluid);
			put32bit(&ptr,eaptr->sesdata->mapallgid);
			if (vmode) {
				put8bit(&ptr,eaptr->sesdata->mingoal);
				put8bit(&ptr,eaptr->sesdata->maxgoal);
				put32bit(&ptr,eaptr->sesdata->mintrashtime);
				put32bit(&ptr,eaptr->sesdata->maxtrashtime);
			}
			if (eaptr->sesdata) {
				for (i=0 ; i<SESSION_STATS ; i++) {
					put32bit(&ptr,eaptr->sesdata->currentopstats[i]);
				}
				for (i=0 ; i<SESSION_STATS ; i++) {
					put32bit(&ptr,eaptr->sesdata->lasthouropstats[i]);
				}
			} else {
				memset(ptr,0xFF,8*SESSION_STATS);
				ptr+=8*SESSION_STATS;
			}
		}
	}
}

void matoclserv_chart(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t chartid;
	uint8_t *ptr;
	uint32_t l;

	if (length!=4) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOAN_CHART - wrong size (%" PRIu32 "/4)",length);
		eptr->mode = KILL;
		return;
	}
	chartid = get32bit(&data);

	if(chartid <= CHARTS_CSV_CHARTID_BASE){
		l = charts_make_png(chartid);
		ptr = matoclserv_createpacket(eptr,ANTOCL_CHART,l);
		if (l>0) {
			charts_get_png(ptr);
		}
	} else {
		l = charts_make_csv(chartid % CHARTS_CSV_CHARTID_BASE);
		ptr = matoclserv_createpacket(eptr,ANTOCL_CHART,l);
		if (l>0) {
			charts_get_csv(ptr);
		}
	}
}

void matoclserv_chart_data(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t chartid;
	uint8_t *ptr;
	uint32_t l;

	if (length!=4) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOAN_CHART_DATA - wrong size (%" PRIu32 "/4)",length);
		eptr->mode = KILL;
		return;
	}
	chartid = get32bit(&data);
	l = charts_datasize(chartid);
	ptr = matoclserv_createpacket(eptr,ANTOCL_CHART_DATA,l);
	if (l>0) {
		charts_makedata(ptr,chartid);
	}
}

void matoclserv_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	LizardFsStatistics statistics;
	(void)data;
	if (length!=0) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_INFO - wrong size (%" PRIu32 "/0)",length);
		eptr->mode = KILL;
		return;
	}
	statistics.version = lizardfsVersion(LIZARDFS_PACKAGE_VERSION_MAJOR,
			LIZARDFS_PACKAGE_VERSION_MINOR, LIZARDFS_PACKAGE_VERSION_MICRO);
	fs_info(&statistics.totalSpace, &statistics.availableSpace, &statistics.trashSpace,
			&statistics.trashNodes, &statistics.reservedSpace, &statistics.reservedNodes,
			&statistics.allNodes, &statistics.dirNodes, &statistics.fileNodes);
	chunk_info(&statistics.chunks, &statistics.chunkCopies, &statistics.regularCopies);
	statistics.memoryUsage = chartsdata_memusage();
	std::vector<uint8_t> response;
	serializeMooseFsPacket(response, MATOCL_INFO, statistics);
	matoclserv_createpacket(eptr, response);
}

void matoclserv_fstest_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t loopstart,loopend,files,ugfiles,mfiles,chunks,ugchunks,mchunks;
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FSTEST_INFO - wrong size (%" PRIu32 "/0)",length);
		eptr->mode = KILL;
		return;
	}
	std::string report;
	fs_test_getdata(loopstart,loopend,files,ugfiles,mfiles,chunks,ugchunks,mchunks,report);
	ptr = matoclserv_createpacket(eptr,MATOCL_FSTEST_INFO,report.size()+36);
	put32bit(&ptr,loopstart);
	put32bit(&ptr,loopend);
	put32bit(&ptr,files);
	put32bit(&ptr,ugfiles);
	put32bit(&ptr,mfiles);
	put32bit(&ptr,chunks);
	put32bit(&ptr,ugchunks);
	put32bit(&ptr,mchunks);
	put32bit(&ptr,(uint32_t)report.size());
	if (!report.empty()) {
		memcpy(ptr,report.c_str(),report.size());
	}
}

void matoclserv_chunkstest_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_CHUNKSTEST_INFO - wrong size (%" PRIu32 "/0)",length);
		eptr->mode = KILL;
		return;
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_CHUNKSTEST_INFO,52);
	chunk_store_info(ptr);
}

void matoclserv_chunks_matrix(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t matrixid;
	(void)data;
	if (length>1) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_CHUNKS_MATRIX - wrong size (%" PRIu32 "/0|1)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==1) {
		matrixid = get8bit(&data);
	} else {
		matrixid = 0;
	}
	ptr = matoclserv_createpacket(eptr, MATOCL_CHUNKS_MATRIX,
			CHUNK_MATRIX_SIZE * CHUNK_MATRIX_SIZE * sizeof(uint32_t));
	chunk_store_chunkcounters(ptr, matrixid);
}

void matoclserv_exports_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t vmode;
	if (length!=0 && length!=1) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_EXPORTS_INFO - wrong size (%" PRIu32 "/0|1)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==0) {
		vmode = 0;
	} else {
		vmode = get8bit(&data);
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_EXPORTS_INFO,exports_info_size(vmode));
	exports_info_data(vmode,ptr);
}

void matoclserv_mlog_list(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_MLOG_LIST - wrong size (%" PRIu32 "/0)",length);
		eptr->mode = KILL;
		return;
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_MLOG_LIST,matomlserv_mloglist_size());
	matomlserv_mloglist_data(ptr);
}

void matoclserv_metadataservers_list(matoclserventry* eptr, const uint8_t* data, uint32_t length) {
	cltoma::metadataserversList::deserialize(data, length);
	matoclserv_createpacket(eptr, matocl::metadataserversList::build(LIZARDFS_VERSHEX,
			matomlserv_shadows()));
}

void matoclserv_list_tapeservers(matoclserventry* eptr, const uint8_t* data, uint32_t length) {
	cltoma::listTapeservers::deserialize(data, length);
	matoclserv_createpacket(eptr, matocl::listTapeservers::build(matotsserv_get_tapeservers()));
}

static void matoclserv_send_iolimits_cfg(matoclserventry *eptr) {
	MessageBuffer buffer;
	matocl::iolimitsConfig::serialize(buffer, gIoLimitsConfigId,
			gIoLimitsRefreshTime * 1000 * 1000, gIoLimitsSubsystem,
			gIoLimitsDatabase.getGroups());
	matoclserv_createpacket(eptr, buffer);
}

static void matoclserv_broadcast_iolimits_cfg() {
	for (matoclserventry *eptr = matoclservhead; eptr; eptr = eptr->next) {
		if (eptr->iolimits) {
			matoclserv_send_iolimits_cfg(eptr);
		}
	}
}

void matoclserv_ping(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t size;
	deserializeAllMooseFsPacketDataNoHeader(data, length, size);
	matoclserv_createpacket(eptr, ANTOAN_PING_REPLY, size);
}

void matoclserv_fuse_register(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	const uint8_t *rptr;
	uint8_t *wptr;
	uint32_t sessionid;
	uint8_t status;
	uint8_t tools;

	if (starting) {
		eptr->mode = KILL;
		return;
	}
	if (length<64) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER - wrong size (%" PRIu32 "/<64)",length);
		eptr->mode = KILL;
		return;
	}
	tools = (memcmp(data,FUSE_REGISTER_BLOB_TOOLS_NOACL,64)==0)?1:0;
	if (eptr->registered == ClientState::kUnregistered
			&& (memcmp(data,FUSE_REGISTER_BLOB_NOACL,64)==0 || tools)) {
		if (RejectOld) {
			lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/NOACL - rejected (option REJECT_OLD_CLIENTS is set)");
			eptr->mode = KILL;
			return;
		}
		if (tools) {
			if (length!=64 && length!=68) {
				lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/NOACL-TOOLS - wrong size (%" PRIu32 "/64|68)",length);
				eptr->mode = KILL;
				return;
			}
		} else {
			if (length!=68 && length!=72) {
				lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/NOACL-MOUNT - wrong size (%" PRIu32 "/68|72)",length);
				eptr->mode = KILL;
				return;
			}
		}
		rptr = data+64;
		if (tools) {
			sessionid = 0;
			if (length==68) {
				eptr->version = get32bit(&rptr);
			}
		} else {
			sessionid = get32bit(&rptr);
			if (length==72) {
				eptr->version = get32bit(&rptr);
			}
		}
		if (eptr->version<0x010500 && !tools) {
			lzfs_pretty_syslog(LOG_NOTICE,"got register packet from mount older than 1.5 - rejecting");
			eptr->mode = KILL;
			return;
		}
		if (sessionid==0) {     // new session
			status = LIZARDFS_STATUS_OK; // exports_check(eptr->peerip,(const uint8_t*)"",NULL,NULL,&sesflags);      // check privileges for '/' w/o password
				eptr->sesdata = matoclserv_new_session(0,tools);
				if (eptr->sesdata==NULL) {
					lzfs_pretty_syslog(LOG_NOTICE,"can't allocate session record");
					eptr->mode = KILL;
					return;
				}
				eptr->sesdata->rootinode = SPECIAL_INODE_ROOT;
				eptr->sesdata->sesflags = 0;
				eptr->sesdata->peerip = eptr->peerip;
		} else { // reconnect or tools
			eptr->sesdata = matoclserv_find_session(sessionid);
			if (eptr->sesdata==NULL) {      // in old model if session doesn't exist then create it
				eptr->sesdata = matoclserv_new_session(0,0);
				if (eptr->sesdata==NULL) {
					lzfs_pretty_syslog(LOG_NOTICE,"can't allocate session record");
					eptr->mode = KILL;
					return;
				}
				eptr->sesdata->rootinode = SPECIAL_INODE_ROOT;
				eptr->sesdata->sesflags = 0;
				eptr->sesdata->peerip = eptr->peerip;
				status = LIZARDFS_STATUS_OK;
			} else if (eptr->sesdata->peerip==0) { // created by "filesystem"
				eptr->sesdata->peerip = eptr->peerip;
				status = LIZARDFS_STATUS_OK;
			} else if (eptr->sesdata->peerip==eptr->peerip) {
				status = LIZARDFS_STATUS_OK;
			} else {
				status = LIZARDFS_ERROR_EACCES;
			}
		}
		if (tools) {
			wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,1);
		} else {
			wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,(status!=LIZARDFS_STATUS_OK)?1:4);
		}
		if (status!=LIZARDFS_STATUS_OK) {
			put8bit(&wptr,status);
			return;
		}
		if (tools) {
			put8bit(&wptr,status);
		} else {
			sessionid = eptr->sesdata->sessionid;
			put32bit(&wptr,sessionid);
		}
		eptr->registered = (tools) ? ClientState::kOldTools : ClientState::kRegistered;
		return;
	} else if (memcmp(data,FUSE_REGISTER_BLOB_ACL,64)==0) {
		uint32_t rootinode;
		uint8_t sesflags;
		uint8_t mingoal,maxgoal;
		uint32_t mintrashtime,maxtrashtime;
		uint32_t rootuid,rootgid;
		uint32_t mapalluid,mapallgid;
		uint32_t ileng,pleng;
		uint8_t i,rcode;
		const uint8_t *path;
		const char *info;

		if (length<65) {
			lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL - wrong size (%" PRIu32 "/<65)",length);
			eptr->mode = KILL;
			return;
		}

		rptr = data+64;
		rcode = get8bit(&rptr);

		if ((eptr->registered == ClientState::kUnregistered && rcode == REGISTER_CLOSESESSION) ||
		    (eptr->registered != ClientState::kUnregistered && rcode != REGISTER_CLOSESESSION)) {
			lzfs_pretty_syslog(
			    LOG_NOTICE,
			    "CLTOMA_FUSE_REGISTER/ACL - wrong rcode (%d) for registered status (%d)", rcode,
			    (int)eptr->registered);
			eptr->mode = KILL;
			return;
		}

		switch (rcode) {
		case REGISTER_GETRANDOM:
			if (length!=65) {
				lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.1 - wrong size (%" PRIu32 "/65)",length);
				eptr->mode = KILL;
				return;
			}
			wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,32);
			for (i=0 ; i<32 ; i++) {
				eptr->passwordrnd[i]=rnd<uint8_t>();
			}
			memcpy(wptr,eptr->passwordrnd,32);
			return;
		case REGISTER_NEWSESSION:
			if (length<77) {
				lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.2 - wrong size (%" PRIu32 "/>=77)",length);
				eptr->mode = KILL;
				return;
			}
			eptr->version = get32bit(&rptr);
			ileng = get32bit(&rptr);
			if (length<77+ileng) {
				lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.2 - wrong size (%" PRIu32 "/>=77+ileng(%" PRIu32 "))",length,ileng);
				eptr->mode = KILL;
				return;
			}
			info = (const char*)rptr;
			rptr+=ileng;
			pleng = get32bit(&rptr);
			if (length!=77+ileng+pleng && length!=77+16+ileng+pleng) {
				lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.2 - wrong size (%" PRIu32 "/77+ileng(%" PRIu32 ")+pleng(%" PRIu32 ")[+16])",length,ileng,pleng);
				eptr->mode = KILL;
				return;
			}
			path = rptr;
			rptr+=pleng;
			if (pleng>0 && rptr[-1]!=0) {
				lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.2 - received path without ending zero");
				eptr->mode = KILL;
				return;
			}
			if (pleng==0) {
				path = (const uint8_t*)"";
			}
			if (length==77+16+ileng+pleng) {
				status = exports_check(eptr->peerip,eptr->version,0,path,eptr->passwordrnd,rptr,&sesflags,&rootuid,&rootgid,&mapalluid,&mapallgid,&mingoal,&maxgoal,&mintrashtime,&maxtrashtime);
			} else {
				status = exports_check(eptr->peerip,eptr->version,0,path,NULL,NULL,&sesflags,&rootuid,&rootgid,&mapalluid,&mapallgid,&mingoal,&maxgoal,&mintrashtime,&maxtrashtime);
			}
			if (status==LIZARDFS_STATUS_OK) {
				status = fs_getrootinode(&rootinode,path);
			}
			if (status==LIZARDFS_STATUS_OK) {
				eptr->sesdata = matoclserv_new_session(1,0);
				if (eptr->sesdata==NULL) {
					lzfs_pretty_syslog(LOG_NOTICE,"can't allocate session record");
					eptr->mode = KILL;
					return;
				}
				eptr->sesdata->rootinode = rootinode;
				eptr->sesdata->sesflags = sesflags;
				eptr->sesdata->rootuid = rootuid;
				eptr->sesdata->rootgid = rootgid;
				eptr->sesdata->mapalluid = mapalluid;
				eptr->sesdata->mapallgid = mapallgid;
				eptr->sesdata->mingoal = mingoal;
				eptr->sesdata->maxgoal = maxgoal;
				eptr->sesdata->mintrashtime = mintrashtime;
				eptr->sesdata->maxtrashtime = maxtrashtime;
				eptr->sesdata->peerip = eptr->peerip;
				if (ileng>0) {
					if (info[ileng-1]==0) {
						eptr->sesdata->info = strdup(info);
						passert(eptr->sesdata->info);
					} else {
						eptr->sesdata->info = (char*) malloc(ileng+1);
						passert(eptr->sesdata->info);
						memcpy(eptr->sesdata->info,info,ileng);
						eptr->sesdata->info[ileng]=0;
					}
				}
				matoclserv_store_sessions();
			}
			wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,(status==LIZARDFS_STATUS_OK)?((eptr->version>=0x01061A)?35:(eptr->version>=0x010615)?25:(eptr->version>=0x010601)?21:13):1);
			if (status!=LIZARDFS_STATUS_OK) {
				put8bit(&wptr,status);
				return;
			}
			sessionid = eptr->sesdata->sessionid;
			if (eptr->version==0x010615) {
				put32bit(&wptr,0);
			} else if (eptr->version>=0x010616) {
				put16bit(&wptr,LIZARDFS_PACKAGE_VERSION_MAJOR);
				put8bit(&wptr,LIZARDFS_PACKAGE_VERSION_MINOR);
				put8bit(&wptr,LIZARDFS_PACKAGE_VERSION_MICRO);
			}
			put32bit(&wptr,sessionid);
			put8bit(&wptr,sesflags);
			put32bit(&wptr,rootuid);
			put32bit(&wptr,rootgid);
			if (eptr->version>=0x010601) {
				put32bit(&wptr,mapalluid);
				put32bit(&wptr,mapallgid);
			}
			if (eptr->version>=0x01061A) {
				put8bit(&wptr,mingoal);
				put8bit(&wptr,maxgoal);
				put32bit(&wptr,mintrashtime);
				put32bit(&wptr,maxtrashtime);
			}
			if (eptr->version >= lizardfsVersion(1, 6, 30)) {
				eptr->iolimits = true;
				matoclserv_send_iolimits_cfg(eptr);
			}
			eptr->registered = ClientState::kRegistered;
			return;
		case REGISTER_NEWMETASESSION:
			if (length<73) {
				lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.5 - wrong size (%" PRIu32 "/>=73)",length);
				eptr->mode = KILL;
				return;
			}
			eptr->version = get32bit(&rptr);
			ileng = get32bit(&rptr);
			if (length!=73+ileng && length!=73+16+ileng) {
				lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.5 - wrong size (%" PRIu32 "/73+ileng(%" PRIu32 ")[+16])",length,ileng);
				eptr->mode = KILL;
				return;
			}
			info = (const char*)rptr;
			rptr+=ileng;
			if (length==73+16+ileng) {
				status = exports_check(eptr->peerip,eptr->version,1,NULL,eptr->passwordrnd,rptr,&sesflags,&rootuid,&rootgid,&mapalluid,&mapallgid,&mingoal,&maxgoal,&mintrashtime,&maxtrashtime);
			} else {
				status = exports_check(eptr->peerip,eptr->version,1,NULL,NULL,NULL,&sesflags,&rootuid,&rootgid,&mapalluid,&mapallgid,&mingoal,&maxgoal,&mintrashtime,&maxtrashtime);
			}
			if (status==LIZARDFS_STATUS_OK) {
				eptr->sesdata = matoclserv_new_session(1,0);
				if (eptr->sesdata==NULL) {
					lzfs_pretty_syslog(LOG_NOTICE,"can't allocate session record");
					eptr->mode = KILL;
					return;
				}
				eptr->sesdata->rootinode = 0;
				eptr->sesdata->sesflags = sesflags;
				eptr->sesdata->rootuid = 0;
				eptr->sesdata->rootgid = 0;
				eptr->sesdata->mapalluid = 0;
				eptr->sesdata->mapallgid = 0;
				eptr->sesdata->mingoal = mingoal;
				eptr->sesdata->maxgoal = maxgoal;
				eptr->sesdata->mintrashtime = mintrashtime;
				eptr->sesdata->maxtrashtime = maxtrashtime;
				eptr->sesdata->peerip = eptr->peerip;
				if (ileng>0) {
					if (info[ileng-1]==0) {
						eptr->sesdata->info = strdup(info);
						passert(eptr->sesdata->info);
					} else {
						eptr->sesdata->info = (char*) malloc(ileng+1);
						passert(eptr->sesdata->info);
						memcpy(eptr->sesdata->info,info,ileng);
						eptr->sesdata->info[ileng]=0;
					}
				}
				matoclserv_store_sessions();
			}
			wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,(status==LIZARDFS_STATUS_OK)?((eptr->version>=0x01061A)?19:(eptr->version>=0x010615)?9:5):1);
			if (status!=LIZARDFS_STATUS_OK) {
				put8bit(&wptr,status);
				return;
			}
			sessionid = eptr->sesdata->sessionid;
			if (eptr->version>=0x010615) {
				put16bit(&wptr,LIZARDFS_PACKAGE_VERSION_MAJOR);
				put8bit(&wptr,LIZARDFS_PACKAGE_VERSION_MINOR);
				put8bit(&wptr,LIZARDFS_PACKAGE_VERSION_MICRO);
			}
			put32bit(&wptr,sessionid);
			put8bit(&wptr,sesflags);
			if (eptr->version>=0x01061A) {
				put8bit(&wptr,mingoal);
				put8bit(&wptr,maxgoal);
				put32bit(&wptr,mintrashtime);
				put32bit(&wptr,maxtrashtime);
			}
			eptr->registered = ClientState::kRegistered;
			return;
		case REGISTER_RECONNECT:
		case REGISTER_TOOLS:
			if (length<73) {
				lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.%" PRIu8 " - wrong size (%" PRIu32 "/73)",rcode,length);
				eptr->mode = KILL;
				return;
			}
			sessionid = get32bit(&rptr);
			eptr->version = get32bit(&rptr);
			eptr->sesdata = matoclserv_find_session(sessionid);
			if (eptr->sesdata == NULL || eptr->sesdata->peerip == 0) {
				status = LIZARDFS_ERROR_BADSESSIONID;
			} else {
				if ((eptr->sesdata->sesflags&SESFLAG_DYNAMICIP)==0 && eptr->peerip!=eptr->sesdata->peerip) {
					status = LIZARDFS_ERROR_EACCES;
				} else {
					status = LIZARDFS_STATUS_OK;
				}
			}
			wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,1);
			put8bit(&wptr,status);
			if (status!=LIZARDFS_STATUS_OK) {
				return;
			}
			if (rcode == REGISTER_RECONNECT) {
				if (eptr->version >= lizardfsVersion(1, 6, 30) && eptr->sesdata->rootinode != 0) {
					eptr->iolimits = true;
					matoclserv_send_iolimits_cfg(eptr);
				}
				eptr->registered = ClientState::kRegistered;
			} else {
				eptr->registered = ClientState::kOldTools;
			}
			return;
		case REGISTER_CLOSESESSION:
			if (length<69) {
				lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.6 - wrong size (%" PRIu32 "/69)",length);
				eptr->mode = KILL;
				return;
			}
			sessionid = get32bit(&rptr);
			matoclserv_close_session(sessionid);
			eptr->mode = KILL;
			return;
		}
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL - wrong rcode (%" PRIu8 ")",rcode);
		eptr->mode = KILL;
		return;
	} else {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER - wrong register blob");
		eptr->mode = KILL;
		return;
	}
}

void matoclserv_fuse_reserved_inodes(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	const uint8_t *ptr;
	filelist *ofptr,**ofpptr;
	uint32_t inode;

	if ((length&0x3)!=0) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_RESERVED_INODES - wrong size (%" PRIu32 "/N*4)",length);
		eptr->mode = KILL;
		return;
	}

	ptr = data;
	ofpptr = &(eptr->sesdata->openedfiles);
	length >>= 2;
	if (length) {
		length--;
		inode = get32bit(&ptr);
	} else {
		inode=0;
	}

	FsContext context = FsContext::getForMaster(eventloop_time());
	changelog_disable_flush();
	while ((ofptr=*ofpptr) && inode>0) {
		if (ofptr->inode<inode) {
			fs_release(context, ofptr->inode, eptr->sesdata->sessionid);
			*ofpptr = ofptr->next;
			free(ofptr);
		} else if (ofptr->inode>inode) {
			if (fs_acquire(context, inode, eptr->sesdata->sessionid) == LIZARDFS_STATUS_OK) {
				ofptr = (filelist*)malloc(sizeof(filelist));
				passert(ofptr);
				ofptr->next = *ofpptr;
				ofptr->inode = inode;
				*ofpptr = ofptr;
				ofpptr = &(ofptr->next);
			}
			if (length) {
				length--;
				inode = get32bit(&ptr);
			} else {
				inode=0;
			}
		} else {
			ofpptr = &(ofptr->next);
			if (length) {
				length--;
				inode = get32bit(&ptr);
			} else {
				inode=0;
			}
		}
	}
	while (inode>0) {
		if (fs_acquire(context, inode, eptr->sesdata->sessionid) == LIZARDFS_STATUS_OK) {
			ofptr = (filelist*)malloc(sizeof(filelist));
			passert(ofptr);
			ofptr->next = *ofpptr;
			ofptr->inode = inode;
			*ofpptr = ofptr;
			ofpptr = &(ofptr->next);
		}
		if (length) {
			length--;
			inode = get32bit(&ptr);
		} else {
			inode=0;
		}
	}
	while ((ofptr=*ofpptr)) {
		fs_release(context, ofptr->inode, eptr->sesdata->sessionid);
		*ofpptr = ofptr->next;
		free(ofptr);
	}
	changelog_enable_flush();
}

void matoclserv_fuse_statfs(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t totalspace,availspace,trashspace,reservedspace;
	uint32_t msgid,inodes;
	uint8_t *ptr;
	if (length!=4) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_STATFS - wrong size (%" PRIu32 "/4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	FsContext context = matoclserv_get_context(eptr);
	fs_statfs(context,&totalspace,&availspace,&trashspace,&reservedspace,&inodes);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_STATFS,40);
	put32bit(&ptr,msgid);
	put64bit(&ptr,totalspace);
	put64bit(&ptr,availspace);
	put64bit(&ptr,trashspace);
	put64bit(&ptr,reservedspace);
	put32bit(&ptr,inodes);
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[0]++;
	}
}

void matoclserv_fuse_access(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint8_t modemask;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=17) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_ACCESS - wrong size (%" PRIu32 "/17)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	gid = get32bit(&data);
	modemask = get8bit(&data);
	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);
		status = fs_access(context,inode,modemask);
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_ACCESS,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_liz_whole_path_lookup(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t msgid;
	uint32_t inode, found_inode;
	std::string path;
	uint32_t uid, gid;
	Attributes attr;
	uint8_t status = LIZARDFS_STATUS_OK;

	cltoma::wholePathLookup::deserialize(data, length, msgid, inode, path, uid, gid);

	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);
		status = fs_whole_path_lookup(context, inode, path, &found_inode, attr);
	}

	if (status != LIZARDFS_STATUS_OK) {
		matoclserv_createpacket(eptr, matocl::wholePathLookup::build(msgid, status));
	} else {
		matoclserv_createpacket(eptr, matocl::wholePathLookup::build(msgid, found_inode, attr));
	}
	eptr->sesdata->currentopstats[3]++;
}

void matoclserv_fuse_lookup(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint8_t nleng;
	const uint8_t *name;
	uint32_t newinode;
	Attributes attr;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<17) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_LOOKUP - wrong size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=17U+nleng) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_LOOKUP - wrong size (%" PRIu32 ":nleng=%" PRIu8 ")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	uid = get32bit(&data);
	gid = get32bit(&data);
	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);
		status = fs_lookup(context,inode,HString((char*)name, nleng),&newinode,attr);
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_LOOKUP,(status!=LIZARDFS_STATUS_OK)?5:43);
	put32bit(&ptr,msgid);
	if (status!=LIZARDFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,newinode);
		memcpy(ptr, attr.data(), attr.size());
	}
	eptr->sesdata->currentopstats[3]++;
}

void matoclserv_fuse_getattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	Attributes attr;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=16) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_GETATTR - wrong size (%" PRIu32 "/16)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	gid = get32bit(&data);
	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);
		status = fs_getattr(context,inode,attr);
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETATTR,(status!=LIZARDFS_STATUS_OK)?5:39);
	put32bit(&ptr,msgid);
	if (status!=LIZARDFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		memcpy(ptr, attr.data(), attr.size());
	}
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[1]++;
	}
}

void matoclserv_fuse_setattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint16_t setmask;
	Attributes attr;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	SugidClearMode sugidclearmode;
	uint16_t attrmode;
	uint32_t attruid,attrgid,attratime,attrmtime;
	if (length!=35 && length!=36) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_SETATTR - wrong size (%" PRIu32 "/35|36)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	gid = get32bit(&data);
	setmask = get8bit(&data);
	attrmode = get16bit(&data);
	attruid = get32bit(&data);
	attrgid = get32bit(&data);
	attratime = get32bit(&data);
	attrmtime = get32bit(&data);
	if (length==36) {
		sugidclearmode = static_cast<SugidClearMode>(get8bit(&data));
	} else {
		sugidclearmode = SugidClearMode::kAlways; // this is safest option
	}
	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);
		status = fs_setattr(context, inode, setmask, attrmode, attruid, attrgid,
							attratime, attrmtime, sugidclearmode, attr);
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SETATTR,(status!=LIZARDFS_STATUS_OK)?5:39);
	put32bit(&ptr,msgid);
	if (status!=LIZARDFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		memcpy(ptr, attr.data(), attr.size());
	}
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[2]++;
	}
}

void matoclserv_fuse_truncate(matoclserventry *eptr, PacketHeader header, const uint8_t *data) {
	sassert(header.type == CLTOMA_FUSE_TRUNCATE
			|| header.type == LIZ_CLTOMA_FUSE_TRUNCATE
			|| header.type == LIZ_CLTOMA_FUSE_TRUNCATE_END);

	// Deserialize the request
	std::vector<uint8_t> request(data, data + header.length);
	uint8_t status = LIZARDFS_STATUS_OK;
	uint32_t messageId, inode, uid, gid, type;
	uint32_t lockId = 0;
	bool opened;
	uint64_t chunkId, length;
	FsContext context = matoclserv_get_context(eptr);

	const PacketSerializer *serializer = PacketSerializer::getSerializer(header.type, eptr->version);
	if (header.type == LIZ_CLTOMA_FUSE_TRUNCATE_END) {
		cltoma::fuseTruncateEnd::deserialize(request,
				messageId, inode, uid, gid, length, lockId);
		type = FUSE_TRUNCATE_END;
		status = matoclserv_check_group_cache(eptr, gid);
		if (status == LIZARDFS_STATUS_OK) {
			opened = true; // permissions have already been checked on LIZ_CLTOMA_TRUNCATE
			context = matoclserv_get_context(eptr, uid, gid);
			// We have to verify lockid in this request
			if (lockId == 0) { // unlocking with lockid == 0 means "force unlock", this is not allowed
				status = LIZARDFS_ERROR_WRONGLOCKID;
			} else {
				// let's check if chunk is still locked by us
				status = fs_get_chunkid(context, inode, length / MFSCHUNKSIZE, &chunkId);
				if (status == LIZARDFS_STATUS_OK) {
					status = chunk_can_unlock(chunkId, lockId);
				}
				fs_end_setlength(chunkId);
			}
		}
	} else {
		serializer->deserializeFuseTruncate(request, messageId, inode, opened, uid, gid, length);
		type = FUSE_TRUNCATE;
		status = matoclserv_check_group_cache(eptr, gid);
		if (status == LIZARDFS_STATUS_OK) {
			context = matoclserv_get_context(eptr, uid, gid);
		}
	}

	// Try to do the truncate
	Attributes attr;
	if (status == LIZARDFS_STATUS_OK) {
		status = fs_try_setlength(context, inode, opened, length,
								  (type != FUSE_TRUNCATE_END), lockId, attr, &chunkId);
	}

	// In case of LIZARDFS_ERROR_NOTPOSSIBLE we have to tell the client to write the chunk before truncating
	if (status == LIZARDFS_ERROR_NOTPOSSIBLE && header.type == CLTOMA_FUSE_TRUNCATE) {
		// Old client requested to truncate xor chunk. We can't do this!
		status = LIZARDFS_ERROR_ENOTSUP;
	} else if (status == LIZARDFS_ERROR_NOTPOSSIBLE && header.type == LIZ_CLTOMA_FUSE_TRUNCATE) {
		// New client requested to truncate xor chunk. He has to do it himself.
		uint64_t fileLength;
		uint8_t opflag;
		fs_writechunk(context, inode, length / MFSCHUNKSIZE, false,
				&lockId, &chunkId, &opflag, &fileLength);
		if (opflag) {
			// But first we have to duplicate chunk :)
			type = FUSE_TRUNCATE_BEGIN;
			length = fileLength;
			status = LIZARDFS_ERROR_DELAYED;
		} else {
			// No duplication is needed
			std::vector<uint8_t> reply;
			matocl::fuseTruncate::serialize(reply, messageId, fileLength, lockId);
			matoclserv_createpacket(eptr, std::move(reply));
			if (eptr->sesdata) {
				eptr->sesdata->currentopstats[2]++;
			}
			return;
		}
	}

	if (status == LIZARDFS_ERROR_DELAYED) {
		// Duplicate or truncate request has been sent to chunkservers, delay the reply
		chunklist *cl = (chunklist*)malloc(sizeof(chunklist));
		passert(cl);
		cl->chunkid = chunkId;
		cl->qid = messageId;
		cl->inode = inode;
		cl->uid = context.uid();
		cl->gid = context.gid();
		cl->auid = context.auid();
		cl->agid = context.agid();
		cl->fleng = length;
		cl->lockid = lockId;
		cl->type = type;
		cl->serializer = serializer;
		cl->next = eptr->chunkdelayedops;
		eptr->chunkdelayedops = cl;
		if (eptr->sesdata) {
			eptr->sesdata->currentopstats[2]++;
		}
		return;
	}
	if (status == LIZARDFS_STATUS_OK) {
		status = fs_do_setlength(context, inode, length, attr);
	}
	if (status == LIZARDFS_STATUS_OK) {
		dcm_modify(inode, eptr->sesdata->sessionid);
	}

	std::vector<uint8_t> reply;
	if (status == LIZARDFS_STATUS_OK) {
		serializer->serializeFuseTruncate(reply, type, messageId, attr);
	} else {
		serializer->serializeFuseTruncate(reply, type, messageId, status);
	}
	matoclserv_createpacket(eptr, std::move(reply));
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[2]++;
	}
}

void matoclserv_fuse_readlink(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	std::string path;
	if (length != 8) {
		lzfs_pretty_syslog(LOG_NOTICE, "CLTOMA_FUSE_READLINK - wrong size (%" PRIu32 "/8)", length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	FsContext context = matoclserv_get_context(eptr);
	status = fs_readlink(context, inode, path);
	ptr = matoclserv_createpacket(eptr, MATOCL_FUSE_READLINK,
	                              (status != LIZARDFS_STATUS_OK) ? 5 : 8 + path.length() + 1);
	put32bit(&ptr, msgid);
	if (status != LIZARDFS_STATUS_OK) {
		put8bit(&ptr, status);
	} else {
		put32bit(&ptr, path.length() + 1);
		if (path.length() > 0) {
			memcpy(ptr, path.c_str(), path.length());
		}
		ptr[path.length()] = 0;
	}
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[7]++;
	}
}

void matoclserv_fuse_symlink(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint8_t nleng;
	const uint8_t *name, *path;
	uint32_t uid, gid;
	uint32_t pleng;
	uint32_t newinode;
	Attributes attr;
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (length < 21) {
		lzfs_pretty_syslog(LOG_NOTICE, "CLTOMA_FUSE_SYMLINK - wrong size (%" PRIu32 ")", length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length < 21U + nleng) {
		lzfs_pretty_syslog(LOG_NOTICE, "CLTOMA_FUSE_SYMLINK - wrong size (%" PRIu32 ":nleng=%" PRIu8 ")",
		       length, nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	pleng = get32bit(&data);
	if (length != 21U + nleng + pleng) {
		lzfs_pretty_syslog(LOG_NOTICE,
		       "CLTOMA_FUSE_SYMLINK - wrong size (%" PRIu32 ":nleng=%" PRIu8 ":pleng=%" PRIu32 ")",
		       length, nleng, pleng);
		eptr->mode = KILL;
		return;
	}
	path = data;
	data += pleng;
	uid = get32bit(&data);
	gid = get32bit(&data);
	while (pleng > 0 && path[pleng - 1] == 0) {
		pleng--;
	}
	newinode = 0;  // request to acquire new inode id
	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		auto context = matoclserv_get_context(eptr, uid, gid);
		status = fs_symlink(context, inode, HString((char *)name, nleng),
	                    std::string((char *)path, pleng), &newinode, &attr);
	}
	ptr =
	    matoclserv_createpacket(eptr, MATOCL_FUSE_SYMLINK, (status != LIZARDFS_STATUS_OK) ? 5 : 43);
	put32bit(&ptr, msgid);
	if (status != LIZARDFS_STATUS_OK) {
		put8bit(&ptr, status);
	} else {
		put32bit(&ptr, newinode);
		memcpy(ptr, attr.data(), attr.size());
	}
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[6]++;
	}
}

void matoclserv_fuse_mknod(matoclserventry *eptr, PacketHeader header, const uint8_t *data) {
	uint32_t messageId, inode, uid, gid, rdev;
	MooseFsString<uint8_t> name;
	uint8_t type;
	uint16_t mode, umask;

	if (header.type == CLTOMA_FUSE_MKNOD) {
		deserializeAllMooseFsPacketDataNoHeader(data, header.length,
				messageId, inode, name, type, mode, uid, gid, rdev);
		umask = 0;
	} else if (header.type == LIZ_CLTOMA_FUSE_MKNOD) {
		cltoma::fuseMknod::deserialize(data, header.length,
				messageId, inode, name, type, mode, umask, uid, gid, rdev);
	} else {
		throw IncorrectDeserializationException(
				"Unknown packet type for matoclserv_fuse_mknod: " + std::to_string(header.type));
	}

	uint32_t newinode;
	Attributes attr;
	uint8_t status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);

		status = fs_mknod(context,
				inode, HString(std::move(name)),
				type, mode, umask, rdev, &newinode, attr);
	}

	MessageBuffer reply;
	if (status == LIZARDFS_STATUS_OK && header.type == CLTOMA_FUSE_MKNOD) {
		serializeMooseFsPacket(reply, MATOCL_FUSE_MKNOD, messageId, newinode, attr);
	} else if (status == LIZARDFS_STATUS_OK && header.type == LIZ_CLTOMA_FUSE_MKNOD) {
		matocl::fuseMknod::serialize(reply, messageId, newinode, attr);
	} else if (header.type == LIZ_CLTOMA_FUSE_MKNOD) {
		matocl::fuseMknod::serialize(reply, messageId, status);
	} else {
		serializeMooseFsPacket(reply, MATOCL_FUSE_MKNOD, messageId, status);
	}
	matoclserv_createpacket(eptr, std::move(reply));
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[8]++;
	}
}

void matoclserv_fuse_mkdir(matoclserventry *eptr, PacketHeader header, const uint8_t *data) {
	uint32_t messageId, inode, uid, gid;
	MooseFsString<uint8_t> name;
	bool copysgid;
	uint16_t mode, umask;

	if (header.type == CLTOMA_FUSE_MKDIR) {
		if (eptr->version >= lizardfsVersion(1, 6, 25)) {
			deserializeAllMooseFsPacketDataNoHeader(data, header.length,
					messageId, inode, name, mode, uid, gid, copysgid);
		} else {
			deserializeAllMooseFsPacketDataNoHeader(data, header.length,
					messageId, inode, name, mode, uid, gid);
			copysgid = false;
		}
		umask = 0;
	} else if (header.type == LIZ_CLTOMA_FUSE_MKDIR) {
		cltoma::fuseMkdir::deserialize(data, header.length, messageId,
				inode, name, mode, umask, uid, gid, copysgid);
	} else {
		throw IncorrectDeserializationException(
				"Unknown packet type for matoclserv_fuse_mkdir: " + std::to_string(header.type));
	}

	uint32_t newinode;
	Attributes attr;
	uint8_t status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);

		status = fs_mkdir(context, inode, HString(std::move(name)), mode, umask,
						copysgid, &newinode, attr);
	}

	MessageBuffer reply;
	if (status == LIZARDFS_STATUS_OK && header.type == CLTOMA_FUSE_MKDIR) {
		serializeMooseFsPacket(reply, MATOCL_FUSE_MKDIR, messageId, newinode, attr);
	} else if (status == LIZARDFS_STATUS_OK && header.type == LIZ_CLTOMA_FUSE_MKDIR) {
		matocl::fuseMkdir::serialize(reply, messageId, newinode, attr);
	} else if (header.type == LIZ_CLTOMA_FUSE_MKDIR) {
		matocl::fuseMkdir::serialize(reply, messageId, status);
	} else {
		serializeMooseFsPacket(reply, MATOCL_FUSE_MKDIR, messageId, status);
	}
	matoclserv_createpacket(eptr, std::move(reply));
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[4]++;
	}
}

void matoclserv_fuse_unlink(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint8_t nleng;
	const uint8_t *name;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<17) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_UNLINK - wrong size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=17U+nleng) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_UNLINK - wrong size (%" PRIu32 ":nleng=%" PRIu8 ")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	uid = get32bit(&data);
	gid = get32bit(&data);
	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);
		status = fs_unlink(context,inode, HString((char*)name, nleng));
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_UNLINK,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[9]++;
	}
}

void matoclserv_fuse_recursive_remove_wake_up(uint32_t session_id, uint32_t msgid, int status) {
	matoclserventry *eptr = matoclserv_find_connection(session_id);
	if (!eptr) {
		return;
	}
	matoclserv_createpacket(eptr, matocl::recursiveRemove::build(msgid, status));
}

void matoclserv_fuse_recursive_remove(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t parent_inode, uid, gid;
	uint32_t msgid;
	uint8_t status;

	std::string name;
	uint32_t job_id;
	cltoma::recursiveRemove::deserialize(data, length, msgid, job_id, parent_inode, name, uid, gid);

	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);

		status = fs_recursive_remove(context, parent_inode, HString(name),
					    std::bind(matoclserv_fuse_recursive_remove_wake_up,
				      eptr->sesdata->sessionid, msgid, std::placeholders::_1), job_id);
	}
	if (status != LIZARDFS_ERROR_WAITING) {
		matoclserv_createpacket(eptr, matocl::recursiveRemove::build(msgid, status));
	}
}

void matoclserv_fuse_rmdir(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint8_t nleng;
	const uint8_t *name;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<17) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_RMDIR - wrong size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=17U+nleng) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_RMDIR - wrong size (%" PRIu32 ":nleng=%" PRIu8 ")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	uid = get32bit(&data);
	gid = get32bit(&data);
	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);
		status = fs_rmdir(context,inode,HString((char*)name, nleng));
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_RMDIR,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[5]++;
	}
}

void matoclserv_fuse_rename(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inode_src,inode_dst;
	uint8_t nleng_src,nleng_dst;
	const uint8_t *name_src,*name_dst;
	uint32_t uid,gid;
	Attributes attr;
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (length<22) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_RENAME - wrong size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode_src = get32bit(&data);
	nleng_src = get8bit(&data);
	if (length<22U+nleng_src) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_RENAME - wrong size (%" PRIu32 ":nleng_src=%" PRIu8 ")",length,nleng_src);
		eptr->mode = KILL;
		return;
	}
	name_src = data;
	data += nleng_src;
	inode_dst = get32bit(&data);
	nleng_dst = get8bit(&data);
	if (length!=22U+nleng_src+nleng_dst) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_RENAME - wrong size (%" PRIu32 ":nleng_src=%" PRIu8 ":nleng_dst=%" PRIu8 ")",length,nleng_src,nleng_dst);
		eptr->mode = KILL;
		return;
	}
	name_dst = data;
	data += nleng_dst;
	uid = get32bit(&data);
	gid = get32bit(&data);
	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		auto context = matoclserv_get_context(eptr, uid, gid);
		status = fs_rename(context, inode_src, HString((char*)name_src, nleng_src),
		                   inode_dst, HString((char*)name_dst, nleng_dst), &inode, &attr);
	}
	if (eptr->version>=0x010615 && status==LIZARDFS_STATUS_OK) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_RENAME,43);
	} else {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_RENAME,5);
	}
	put32bit(&ptr,msgid);
	if (eptr->version>=0x010615 && status==LIZARDFS_STATUS_OK) {
		put32bit(&ptr,inode);
		memcpy(ptr, attr.data(), attr.size());
	} else {
		put8bit(&ptr,status);
	}
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[10]++;
	}
}

void matoclserv_fuse_link(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inode_dst;
	uint8_t nleng_dst;
	const uint8_t *name_dst;
	uint32_t uid,gid;
	uint32_t newinode;
	Attributes attr;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<21) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_LINK - wrong size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	inode_dst = get32bit(&data);
	nleng_dst = get8bit(&data);
	if (length!=21U+nleng_dst) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_LINK - wrong size (%" PRIu32 ":nleng_dst=%" PRIu8 ")",length,nleng_dst);
		eptr->mode = KILL;
		return;
	}
	name_dst = data;
	data += nleng_dst;
	uid = get32bit(&data);
	gid = get32bit(&data);
	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		auto context = matoclserv_get_context(eptr, uid, gid);
		status = fs_link(context, inode, inode_dst, HString((char*)name_dst, nleng_dst), &newinode, &attr);
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_LINK,(status!=LIZARDFS_STATUS_OK)?5:43);
	put32bit(&ptr,msgid);
	if (status!=LIZARDFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,newinode);
		memcpy(ptr, attr.data(), attr.size());
	}
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[11]++;
	}
}

void matoclserv_fuse_getdir(matoclserventry *eptr,const PacketHeader &header, const uint8_t *data) {
	uint32_t message_id, inode, uid, gid;
	uint64_t first_entry, number_of_entries;
	MessageBuffer buffer;

	PacketVersion packet_version;
	deserializePacketVersionNoHeader(data, header.length, packet_version);

	if (packet_version == cltoma::fuseGetDir::kClientAbleToProcessDirentIndex) {
		cltoma::fuseGetDir::deserialize(data, header.length, message_id, inode, uid, gid, first_entry, number_of_entries);
	} else if (packet_version == cltoma::fuseGetDirLegacy::kLegacyClient) {
		cltoma::fuseGetDirLegacy::deserialize(data, header.length, message_id, inode, uid, gid, first_entry, number_of_entries);
	} else {
		throw IncorrectDeserializationException(
				"Unknown LIZ_CLTOMA_FUSE_GETDIR version: " + std::to_string(packet_version));
	}

	number_of_entries = std::min(number_of_entries, matocl::fuseGetDir::kMaxNumberOfDirectoryEntries);
	uint8_t status = matoclserv_check_group_cache(eptr, gid);

	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);

		if (packet_version == cltoma::fuseGetDir::kClientAbleToProcessDirentIndex) {
			std::vector<DirectoryEntry> dir_entries;
			status = fs_readdir(context, inode, first_entry, number_of_entries, dir_entries); //<DirectoryEntry>

			if (status != LIZARDFS_STATUS_OK) {
				matocl::fuseGetDir::serialize(buffer, message_id, status);
			} else {
				matocl::fuseGetDir::serialize(buffer, message_id, first_entry, dir_entries);
			}
		} else if (packet_version == cltoma::fuseGetDirLegacy::kLegacyClient) {
			std::vector<legacy::DirectoryEntry> dir_entries;
			status = fs_readdir(context, inode, first_entry, number_of_entries, dir_entries); //<legacy::DirectoryEntry>

			if (status != LIZARDFS_STATUS_OK) {
				matocl::fuseGetDir::serialize(buffer, message_id, status);
			} else {
				matocl::fuseGetDirLegacy::serialize(buffer, message_id, first_entry, dir_entries);
			}
		} else {
			throw IncorrectDeserializationException(
					"Unknown LIZ_CLTOMA_FUSE_GETDIR version: " + std::to_string(packet_version));
		}
	} else {
		matocl::fuseGetDir::serialize(buffer, message_id, status);
	}

	eptr->sesdata->currentopstats[12]++;
	matoclserv_createpacket(eptr, std::move(buffer));
}

void matoclserv_fuse_getdir(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint8_t flags;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint32_t dleng;
	void *custom;
	if (length!=16 && length!=17) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_GETDIR - wrong size (%" PRIu32 "/16|17)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	gid = get32bit(&data);
	if (length==17) {
		flags = get8bit(&data);
	} else {
		flags = 0;
	}

	status = matoclserv_check_group_cache(eptr, gid);
	if (status != LIZARDFS_STATUS_OK) {
		ptr = matoclserv_createpacket(eptr, MATOCL_FUSE_GETDIR, 5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
		eptr->sesdata->currentopstats[12]++;
		return;
	}

	FsContext context = matoclserv_get_context(eptr, uid, gid);
	status = fs_readdir_size(context,inode,flags,&custom,&dleng);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETDIR,(status!=LIZARDFS_STATUS_OK)?5:4+dleng);
	put32bit(&ptr,msgid);
	if (status!=LIZARDFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		fs_readdir_data(context,flags,custom,ptr);
	}

	eptr->sesdata->currentopstats[12]++;
}

void matoclserv_fuse_open(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint8_t flags;
	Attributes attr;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	int allowcache;
	if (length!=17) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_OPEN - wrong size (%" PRIu32 "/17)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	gid = get32bit(&data);
	flags = get8bit(&data);

	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);
		status = matoclserv_insert_openfile(eptr->sesdata,inode);
		if (status==LIZARDFS_STATUS_OK) {
			status = fs_opencheck(context,inode,flags,attr);
		}
	}
	if (eptr->version>=0x010609 && status==LIZARDFS_STATUS_OK) {
		allowcache = dcm_open(inode,eptr->sesdata->sessionid);
		if (allowcache==0) {
			attr[1]&=(0xFF^(MATTR_ALLOWDATACACHE<<4));
		}
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_OPEN,39);
	} else {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_OPEN,5);
	}
	put32bit(&ptr,msgid);
	if (eptr->version>=0x010609 && status==LIZARDFS_STATUS_OK) {
		memcpy(ptr, attr.data(), attr.size());
	} else {
		put8bit(&ptr,status);
	}
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[13]++;
	}
}

void matoclserv_fuse_read_chunk(matoclserventry *eptr, PacketHeader header, const uint8_t *data) {
	sassert(header.type == CLTOMA_FUSE_READ_CHUNK || header.type == LIZ_CLTOMA_FUSE_READ_CHUNK);
	uint8_t status;
	uint64_t chunkid;
	uint64_t fleng;
	uint32_t version;
	uint32_t messageId;
	uint32_t inode;
	uint32_t index;
	std::vector<uint8_t> outMessage;
	const PacketSerializer* serializer = PacketSerializer::getSerializer(header.type, eptr->version);

	std::vector<uint8_t> receivedData(data, data + header.length);
	serializer->deserializeFuseReadChunk(receivedData, messageId, inode, index);

	status = fs_readchunk(inode, index, &chunkid, &fleng);
	std::vector<ChunkTypeWithAddress> allChunkCopies;
	if (status == LIZARDFS_STATUS_OK) {
		if (chunkid > 0) {
			status = chunk_getversionandlocations(chunkid, eptr->peerip, version,
					kMaxNumberOfChunkCopies, allChunkCopies);
			remove_unsupported_ec_parts(eptr->version, allChunkCopies);
		} else {
			version = 0;
		}
	}

	if (status != LIZARDFS_STATUS_OK) {
		serializer->serializeFuseReadChunk(outMessage, messageId, status);
		matoclserv_createpacket(eptr, outMessage);
		return;
	}

	dcm_access(inode, eptr->sesdata->sessionid);
	serializer->serializeFuseReadChunk(outMessage, messageId, fleng, chunkid, version,
			allChunkCopies);
	matoclserv_createpacket(eptr, outMessage);

	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[14]++;
	}
}

void matoclserv_chunks_info(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t message_id, inode, chunk_index, chunk_count, uid, gid;
	PacketVersion version;
	uint8_t status;
	std::vector<ChunkWithAddressAndLabel> chunks;

	deserializePacketVersionNoHeader(data, length, version);
	if (version != cltoma::chunksInfo::kMultiChunk) {
		matoclserv_createpacket(eptr, matocl::chunksInfo::build(message_id, (uint8_t)LIZARDFS_ERROR_EINVAL));
		return;
	}

	cltoma::chunksInfo::deserialize(data, length, message_id, uid, gid, inode, chunk_index, chunk_count);

	chunk_count = std::max<uint32_t>(chunk_count, 1);
	chunk_count = std::min(chunk_count, matocl::chunksInfo::kMaxNumberOfResultEntries);

	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);
		status = fs_getchunksinfo(context, eptr->peerip, inode, chunk_index, chunk_count, chunks);
	}

	if (status != LIZARDFS_STATUS_OK) {
		matoclserv_createpacket(eptr, matocl::chunksInfo::build(message_id, status));
		return;
	}

	matoclserv_createpacket(eptr, matocl::chunksInfo::build(message_id, chunks));
}

void matoclserv_tape_info(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t messageId;
	uint32_t inode;
	cltoma::tapeInfo::deserialize(data, length, messageId, inode);

	std::vector<TapeCopyLocationInfo> tapeLocations;
	uint8_t status = fs_get_tape_copy_locations(inode, tapeLocations);
	if (status != LIZARDFS_STATUS_OK) {
		matoclserv_createpacket(eptr, matocl::tapeInfo::build(messageId, status));
	} else {
		matoclserv_createpacket(eptr, matocl::tapeInfo::build(messageId, tapeLocations));
	}
}

void matoclserv_fuse_write_chunk(matoclserventry *eptr, PacketHeader header, const uint8_t *data) {
	sassert(header.type == CLTOMA_FUSE_WRITE_CHUNK || header.type == LIZ_CLTOMA_FUSE_WRITE_CHUNK);
	uint8_t status;
	uint32_t inode;
	uint32_t chunkIndex;
	uint64_t fileLength;
	uint64_t chunkId;
	uint32_t lockId;
	uint32_t messageId;
	uint8_t opflag;
	chunklist *cl;
	std::vector<uint8_t> outMessage;
	const PacketSerializer* serializer = PacketSerializer::getSerializer(header.type, eptr->version);

	std::vector<uint8_t> receivedData(data, data + header.length);
	serializer->deserializeFuseWriteChunk(receivedData, messageId, inode, chunkIndex, lockId);

	uint32_t min_server_version
		= header.type == LIZ_CLTOMA_FUSE_WRITE_CHUNK ? kFirstXorVersion : 0;

	// Original MooseFS (1.6.27) does not use lock ID's
	bool useDummyLockId = (header.type == CLTOMA_FUSE_WRITE_CHUNK);
	status = fs_writechunk(matoclserv_get_context(eptr), inode, chunkIndex, useDummyLockId,
			&lockId, &chunkId, &opflag, &fileLength, min_server_version);

	if (status != LIZARDFS_STATUS_OK) {
		serializer->serializeFuseWriteChunk(outMessage, messageId, status);
		matoclserv_createpacket(eptr, outMessage);
		return;
	}

	if (opflag) {   // wait for operation end
		cl = (chunklist*)malloc(sizeof(chunklist));
		passert(cl);
		memset(cl, 0, sizeof(chunklist));
		cl->inode = inode;
		cl->chunkid = chunkId;
		cl->qid = messageId;
		cl->fleng = fileLength;
		cl->lockid = lockId;
		cl->type = FUSE_WRITE;
		cl->next = eptr->chunkdelayedops;
		cl->serializer = serializer;
		eptr->chunkdelayedops = cl;
	} else {        // return status immediately
		dcm_modify(inode,eptr->sesdata->sessionid);
		status = matoclserv_fuse_write_chunk_respond(eptr, serializer,
				chunkId, messageId, fileLength, lockId);
		if (status != LIZARDFS_STATUS_OK) {
			fs_writeend(0, 0, chunkId, 0);  // ignore status - just do it.
		}
	}

	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[15]++;
	}
}

void matoclserv_fuse_write_chunk_end(matoclserventry *eptr,
		PacketHeader header, const uint8_t *data) {
	sassert(header.type == CLTOMA_FUSE_WRITE_CHUNK_END
			|| header.type == LIZ_CLTOMA_FUSE_WRITE_CHUNK_END);
	uint32_t messageId;
	uint64_t chunkId;
	uint32_t lockId;
	uint32_t inode;
	uint64_t fileLength;
	uint8_t status;
	std::vector<uint8_t> outMessage;

	std::vector<uint8_t> request(data, data + header.length);
	const PacketSerializer* serializer = PacketSerializer::getSerializer(header.type, eptr->version);
	serializer->deserializeFuseWriteChunkEnd(request, messageId, chunkId, lockId, inode, fileLength);
	if (lockId == 0) {
		// this lock id passed to chunk_unlock would force chunk unlock
		status = LIZARDFS_ERROR_WRONGLOCKID;
	} else if (eptr->sesdata->sesflags & SESFLAG_READONLY) {
		status = LIZARDFS_ERROR_EROFS;
	} else {
		status = fs_writeend(inode, fileLength, chunkId, lockId);
	}
	dcm_modify(inode,eptr->sesdata->sessionid);
	serializer->serializeFuseWriteChunkEnd(outMessage, messageId, status);
	matoclserv_createpacket(eptr, outMessage);
}

void matoclserv_fuse_repair(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t inode, uid, gid;
	uint32_t msgid;
	uint32_t chunksnotchanged, chunkserased, chunksrepaired;
	uint8_t *ptr;
	uint8_t status;
	uint8_t correct_only = 0;
	if (length == 16 || length == 17) {
		msgid = get32bit(&data);
		inode = get32bit(&data);
		uid = get32bit(&data);
		gid = get32bit(&data);
		if (length == 17) {
			correct_only = get8bit(&data);
		}
	}
	else {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_REPAIR - wrong package size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}

	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);
		status = fs_repair(context, inode, correct_only,
				&chunksnotchanged, &chunkserased, &chunksrepaired);
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REPAIR,(status!=LIZARDFS_STATUS_OK)?5:16);
	put32bit(&ptr,msgid);
	if (status!=0) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,chunksnotchanged);
		put32bit(&ptr,chunkserased);
		put32bit(&ptr,chunksrepaired);
	}
}

void matoclserv_fuse_check(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t chunkcount[CHUNK_MATRIX_SIZE];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_CHECK - wrong size (%" PRIu32 "/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_checkfile(matoclserv_get_context(eptr),inode,chunkcount);
	if (status!=LIZARDFS_STATUS_OK) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_CHECK,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	} else {
		if (eptr->version>=0x010617) {
			ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_CHECK,4 + CHUNK_MATRIX_SIZE * 4);
			put32bit(&ptr,msgid);
			for (uint32_t i = 0; i < CHUNK_MATRIX_SIZE; i++) {
				put32bit(&ptr,chunkcount[i]);
			}
		} else {
			uint8_t j;
			j=0;
			for (uint32_t i = 0; i < CHUNK_MATRIX_SIZE; i++) {
				if (chunkcount[i]>0) {
					j++;
				}
			}
			ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_CHECK,4+3*j);
			put32bit(&ptr,msgid);
			for (uint32_t i = 0; i < CHUNK_MATRIX_SIZE; i++) {
				if (chunkcount[i]>0) {
					put8bit(&ptr,i);
					if (chunkcount[i]<=65535) {
						put16bit(&ptr,chunkcount[i]);
					} else {
						put16bit(&ptr,65535);
					}
				}
			}
		}
	}
}

void matoclserv_fuse_request_task_id(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t msgid, taskid;
	cltoma::requestTaskId::deserialize(data, length, msgid);
	taskid = fs_reserve_job_id();
	MessageBuffer reply;
	matocl::requestTaskId::serialize(reply, msgid, taskid);
	matoclserv_createpacket(eptr, reply);
}

void matoclserv_fuse_gettrashtime(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint8_t gmode;
	TrashtimeMap fileTrashtimes, dirTrashtimes;
	uint32_t fileTrashtimesSize, dirTrashtimesSize;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=9) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_GETTRASHTIME - wrong size (%" PRIu32 "/9)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	gmode = get8bit(&data);
	status = fs_gettrashtime_prepare(matoclserv_get_context(eptr), inode, gmode, fileTrashtimes, dirTrashtimes);
	fileTrashtimesSize = fileTrashtimes.size();
	dirTrashtimesSize = dirTrashtimes.size();
	ptr = matoclserv_createpacket(eptr,
				MATOCL_FUSE_GETTRASHTIME,
				(status != LIZARDFS_STATUS_OK) ? 5 : 12 + 8 * (fileTrashtimesSize + dirTrashtimesSize));
	put32bit(&ptr,msgid);
	if (status!=LIZARDFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr, fileTrashtimesSize);
		put32bit(&ptr, dirTrashtimesSize);
		fs_gettrashtime_store(fileTrashtimes, dirTrashtimes, ptr);
	}
}

void matoclserv_fuse_settrashtime_wake_up(uint32_t session_id, uint32_t msgid,
					  std::shared_ptr<SetTrashtimeTask::StatsArray> settrashtime_stats,
					  uint8_t status) {
	matoclserventry *eptr = matoclserv_find_connection(session_id);
	if (!eptr) {
		return;
	}

	MessageBuffer reply;
	if (status != LIZARDFS_STATUS_OK) {
		serializeMooseFsPacket(reply, MATOCL_FUSE_SETTRASHTIME, msgid, status);
	} else {
		uint32_t changed, notchanged, notpermitted;
		changed = (*settrashtime_stats)[SetTrashtimeTask::kChanged];
		notchanged = (*settrashtime_stats)[SetTrashtimeTask::kNotChanged];
		notpermitted = (*settrashtime_stats)[SetTrashtimeTask::kNotPermitted];
		serializeMooseFsPacket(reply, MATOCL_FUSE_SETTRASHTIME, msgid, changed,
				       notchanged, notpermitted);
	}
	matoclserv_createpacket(eptr, std::move(reply));
}

void matoclserv_fuse_settrashtime(matoclserventry *eptr, PacketHeader header, const uint8_t *data) {
	uint32_t inode, uid, trashtime, msgid;
	uint8_t smode, status;

	deserializeAllMooseFsPacketDataNoHeader(data, header.length, msgid, inode,
							uid, trashtime, smode);
// limits check
	status = LIZARDFS_STATUS_OK;
	switch (smode & SMODE_TMASK) {
	case SMODE_SET:
		if (trashtime < eptr->sesdata->mintrashtime || trashtime > eptr->sesdata->maxtrashtime) {
			status = LIZARDFS_ERROR_EPERM;
		}
		break;
	case SMODE_INCREASE:
		if (trashtime > eptr->sesdata->maxtrashtime) {
			status = LIZARDFS_ERROR_EPERM;
		}
		break;
	case SMODE_DECREASE:
		if (trashtime < eptr->sesdata->mintrashtime) {
			status = LIZARDFS_ERROR_EPERM;
		}
		break;
	}

	// array for settrashtime operation statistics
	auto settrashtime_stats = std::make_shared<SetTrashtimeTask::StatsArray>();

	if (status == LIZARDFS_STATUS_OK) {
		status = fs_settrashtime(matoclserv_get_context(eptr, uid, 0), inode, trashtime,
					 smode, settrashtime_stats,
			   std::bind(matoclserv_fuse_settrashtime_wake_up, eptr->sesdata->sessionid,
				     msgid, settrashtime_stats, std::placeholders::_1));
	}

	if (status != LIZARDFS_ERROR_WAITING) {
		matoclserv_fuse_settrashtime_wake_up(eptr->sesdata->sessionid, msgid,
						     settrashtime_stats, status);
	}
}

void matoclserv_fuse_getgoal(matoclserventry *eptr, PacketHeader header, const uint8_t *data) {
	uint32_t inode;
	uint32_t msgid;
	uint8_t gmode;

	if (header.type == CLTOMA_FUSE_GETGOAL) {
		deserializeAllMooseFsPacketDataNoHeader(data, header.length, msgid, inode, gmode);
	} else if (header.type == LIZ_CLTOMA_FUSE_GETGOAL) {
		cltoma::fuseGetGoal::deserialize(data, header.length, msgid, inode, gmode);
	} else {
		throw IncorrectDeserializationException(
				"Unknown packet type for matoclserv_fuse_getgoal: " + std::to_string(header.type));
	}

	GoalStatistics fgtab{{0}}, dgtab{{0}}; // explicit value initialization to clear variables
	uint8_t status = fs_getgoal(matoclserv_get_context(eptr), inode, gmode, fgtab, dgtab);

	MessageBuffer reply;
	if (status == LIZARDFS_STATUS_OK) {
		const std::map<int, Goal>& goalDefinitions = fs_get_goal_definitions();
		std::vector<FuseGetGoalStats> lizReply;
		MooseFSVector<std::pair<uint8_t, uint32_t>> mooseFsReplyFiles, mooseFsReplyDirectories;
		for (const auto &goal : goalDefinitions) {
			if (fgtab[goal.first] || dgtab[goal.first]) {
				lizReply.emplace_back(goal.second.getName(), fgtab[goal.first], dgtab[goal.first]);
			}
			if (fgtab[goal.first] > 0) {
				mooseFsReplyFiles.emplace_back(goal.first, fgtab[goal.first]);
			}
			if (dgtab[goal.first] > 0) {
				mooseFsReplyDirectories.emplace_back(goal.first, dgtab[goal.first]);
			}
		}
		if (header.type == LIZ_CLTOMA_FUSE_GETGOAL) {
			matocl::fuseGetGoal::serialize(reply, msgid, lizReply);
		} else {
			serializeMooseFsPacket(reply, MATOCL_FUSE_GETGOAL,
					msgid,
					uint8_t(mooseFsReplyFiles.size()),
					uint8_t(mooseFsReplyDirectories.size()),
					mooseFsReplyFiles,
					mooseFsReplyDirectories);
		}
	} else {
		if (header.type == LIZ_CLTOMA_FUSE_GETGOAL) {
			matocl::fuseGetGoal::serialize(reply, msgid, status);
		} else {
			serializeMooseFsPacket(reply, MATOCL_FUSE_GETGOAL, msgid, status);
		}
	}
	matoclserv_createpacket(eptr, std::move(reply));
}

void matoclserv_fuse_setgoal_wake_up(uint32_t session_id, uint32_t msgid, uint32_t type,
				     std::shared_ptr<SetGoalTask::StatsArray> setgoal_stats,
				     uint32_t status) {
	matoclserventry *eptr = matoclserv_find_connection(session_id);
	if (!eptr) {
		return;
	}

	MessageBuffer reply;
	if (status == LIZARDFS_STATUS_OK) {
		uint32_t changed, notchanged, notpermitted;
		changed = (*setgoal_stats)[SetGoalTask::kChanged];
		notchanged = (*setgoal_stats)[SetGoalTask::kNotChanged];
		notpermitted = (*setgoal_stats)[SetGoalTask::kNotPermitted];

		if (type == LIZ_CLTOMA_FUSE_SETGOAL) {
			matocl::fuseSetGoal::serialize(reply, msgid, changed, notchanged, notpermitted);
		} else {
			serializeMooseFsPacket(reply, MATOCL_FUSE_SETGOAL,
					msgid, changed, notchanged, notpermitted);
		}
	} else {
		if (type == LIZ_CLTOMA_FUSE_SETGOAL) {
			matocl::fuseSetGoal::serialize(reply, msgid, status);
		} else {
			serializeMooseFsPacket(reply, MATOCL_FUSE_SETGOAL, msgid, status);
		}
	}
	matoclserv_createpacket(eptr, std::move(reply));
}

void matoclserv_fuse_setgoal(matoclserventry *eptr, PacketHeader header, const uint8_t *data) {
	uint32_t inode, uid, msgid;
	uint8_t goalId = 0, smode;
	uint8_t status = LIZARDFS_STATUS_OK;

	if (header.type == CLTOMA_FUSE_SETGOAL) {
		deserializeAllMooseFsPacketDataNoHeader(data, header.length,
				msgid, inode, uid, goalId, smode);
	} else if (header.type == LIZ_CLTOMA_FUSE_SETGOAL) {
		std::string goalName;
		cltoma::fuseSetGoal::deserialize(data, header.length,
				msgid, inode, uid, goalName, smode);
		// find a proper goalId,
		const std::map<int, Goal> &goalDefinitions = fs_get_goal_definitions();
		bool goalFound = false;
		for (const auto &goal : goalDefinitions) {
			if (goal.second.getName() == goalName) {
				goalId = goal.first;
				goalFound = true;
				break;
			}
		}
		if (!goalFound) {
			status = LIZARDFS_ERROR_EINVAL;
		}
	} else {
		throw IncorrectDeserializationException(
				"Unknown packet type for matoclserv_fuse_getgoal: " +
				std::to_string(header.type));
	}

	if (status == LIZARDFS_STATUS_OK && !GoalId::isValid(goalId)) {
		status = LIZARDFS_ERROR_EINVAL;
	}
	if (status == LIZARDFS_STATUS_OK) {
		if (status == LIZARDFS_STATUS_OK && goalId < eptr->sesdata->mingoal) {
			status = LIZARDFS_ERROR_EPERM;
		}
		if (status == LIZARDFS_STATUS_OK && goalId > eptr->sesdata->maxgoal) {
			status = LIZARDFS_ERROR_EPERM;
		}
	}

	// array for setgoal operation statistics
	auto setgoal_stats = std::make_shared<SetGoalTask::StatsArray>();

	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, 0);
		status = fs_setgoal(context, inode, goalId, smode, setgoal_stats,
			   std::bind(matoclserv_fuse_setgoal_wake_up, eptr->sesdata->sessionid,
				     msgid, header.type, setgoal_stats, std::placeholders::_1));
	}

	if (status != LIZARDFS_ERROR_WAITING) {
		matoclserv_fuse_setgoal_wake_up(eptr->sesdata->sessionid, msgid, header.type,
						setgoal_stats, status);
	}
}

void matoclserv_fuse_geteattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint32_t feattrtab[16],deattrtab[16];
	uint8_t i,fn,dn,gmode;
	uint8_t *ptr;
	uint8_t status;
	if (length!=9) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_GETEATTR - wrong size (%" PRIu32 "/9)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	gmode = get8bit(&data);
	status = fs_geteattr(matoclserv_get_context(eptr),inode,gmode,feattrtab,deattrtab);
	fn=0;
	dn=0;
	if (status==LIZARDFS_STATUS_OK) {
		for (i=0 ; i<16 ; i++) {
			if (feattrtab[i]) {
				fn++;
			}
			if (deattrtab[i]) {
				dn++;
			}
		}
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETEATTR,(status!=LIZARDFS_STATUS_OK)?5:6+5*(fn+dn));
	put32bit(&ptr,msgid);
	if (status!=LIZARDFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put8bit(&ptr,fn);
		put8bit(&ptr,dn);
		for (i=0 ; i<16 ; i++) {
			if (feattrtab[i]) {
				put8bit(&ptr,i);
				put32bit(&ptr,feattrtab[i]);
			}
		}
		for (i=0 ; i<16 ; i++) {
			if (deattrtab[i]) {
				put8bit(&ptr,i);
				put32bit(&ptr,deattrtab[i]);
			}
		}
	}
}

void matoclserv_fuse_seteattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid;
	uint32_t msgid;
	uint8_t eattr,smode;
	uint32_t changed,notchanged,notpermitted;
	uint8_t *ptr;
	uint8_t status;
	if (length!=14) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_SETEATTR - wrong size (%" PRIu32 "/14)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	eattr = get8bit(&data);
	smode = get8bit(&data);
	status = fs_seteattr(matoclserv_get_context(eptr, uid, 0), inode, eattr, smode, &changed, &notchanged, &notpermitted);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SETEATTR,(status!=LIZARDFS_STATUS_OK)?5:16);
	put32bit(&ptr,msgid);
	if (status!=LIZARDFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,changed);
		put32bit(&ptr,notchanged);
		put32bit(&ptr,notpermitted);
	}
}

void matoclserv_fuse_getxattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint32_t msgid;
	uint8_t opened;
	uint8_t mode;
	uint8_t *ptr;
	uint8_t status;
	uint8_t anleng;
	const uint8_t *attrname;
	if (length<19) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_GETXATTR - wrong size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	opened = get8bit(&data);
	uid = get32bit(&data);
	gid = get32bit(&data);
	anleng = get8bit(&data);
	attrname = data;
	data+=anleng;
	if (length!=19U+anleng) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_GETXATTR - wrong size (%" PRIu32 ":anleng=%" PRIu8 ")",length,anleng);
		eptr->mode = KILL;
		return;
	}

	status = matoclserv_check_group_cache(eptr, gid);
	if (status != LIZARDFS_STATUS_OK) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETXATTR,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
		return;
	}

	FsContext context = matoclserv_get_context(eptr, uid, gid);

	mode = get8bit(&data);
	if (mode!=XATTR_GMODE_GET_DATA && mode!=XATTR_GMODE_LENGTH_ONLY) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETXATTR,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,LIZARDFS_ERROR_EINVAL);
	} else if (anleng==0) {
		void *xanode;
		uint32_t xasize;
		status = fs_listxattr_leng(context,inode,opened,&xanode,&xasize);
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETXATTR,(status!=LIZARDFS_STATUS_OK)?5:8+((mode==XATTR_GMODE_GET_DATA)?xasize:0));
		put32bit(&ptr,msgid);
		if (status!=LIZARDFS_STATUS_OK) {
			put8bit(&ptr,status);
		} else {
			put32bit(&ptr,xasize);
			if (mode==XATTR_GMODE_GET_DATA && xasize>0) {
				fs_listxattr_data(xanode,ptr);
			}
		}
	} else {
		uint8_t *attrvalue;
		uint32_t avleng;
		status = fs_getxattr(context,inode,opened,anleng,attrname,&avleng,&attrvalue);
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETXATTR,(status!=LIZARDFS_STATUS_OK)?5:8+((mode==XATTR_GMODE_GET_DATA)?avleng:0));
		put32bit(&ptr,msgid);
		if (status!=LIZARDFS_STATUS_OK) {
			put8bit(&ptr,status);
		} else {
			put32bit(&ptr,avleng);
			if (mode==XATTR_GMODE_GET_DATA && avleng>0) {
				memcpy(ptr,attrvalue,avleng);
			}
		}
	}
}

void matoclserv_fuse_setxattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint32_t msgid;
	const uint8_t *attrname,*attrvalue;
	uint8_t opened;
	uint8_t anleng;
	uint32_t avleng;
	uint8_t mode;
	uint8_t *ptr;
	uint8_t status;
	if (length<23) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_SETXATTR - wrong size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	opened = get8bit(&data);
	uid = get32bit(&data);
	gid = get32bit(&data);
	anleng = get8bit(&data);
	if (length<23U+anleng) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_SETXATTR - wrong size (%" PRIu32 ":anleng=%" PRIu8 ")",length,anleng);
		eptr->mode = KILL;
		return;
	}
	attrname = data;
	data += anleng;
	avleng = get32bit(&data);
	if (length!=23U+anleng+avleng) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_SETXATTR - wrong size (%" PRIu32 ":anleng=%" PRIu8 ":avleng=%" PRIu32 ")",length,anleng,avleng);
		eptr->mode = KILL;
		return;
	}
	attrvalue = data;
	data += avleng;
	mode = get8bit(&data);
	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);
		status = fs_setxattr(context,inode,opened,anleng,attrname,avleng,attrvalue,mode);
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SETXATTR,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_append(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t inode,inode_src,uid,gid;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=20) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_APPEND - wrong size (%" PRIu32 "/20)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	inode_src = get32bit(&data);
	uid = get32bit(&data);
	gid = get32bit(&data);
	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		auto context = matoclserv_get_context(eptr, uid, gid);
		status = fs_append(context, inode, inode_src);
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_APPEND,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_snapshot_wake_up(uint32_t type, uint32_t session_id, uint32_t msgid, int status) {
	matoclserventry *eptr = matoclserv_find_connection(session_id);
	if (!eptr) {
		return;
	}

	MessageBuffer buffer;
	if (type == LIZ_CLTOMA_FUSE_SNAPSHOT) {
		matocl::snapshot::serialize(buffer, msgid, status);
	} else {
		serializeMooseFsPacket(buffer, MATOCL_FUSE_SNAPSHOT, msgid, status);
	}
	matoclserv_createpacket(eptr, std::move(buffer));
}

void matoclserv_fuse_snapshot(matoclserventry *eptr, PacketHeader header, const uint8_t *data) {
	uint32_t inode, inode_dst;
	uint32_t uid, gid;
	uint8_t canoverwrite;
	uint32_t msgid;
	uint8_t status;
	uint32_t job_id;
	uint8_t ignore_missing_src = 0;
	uint32_t initial_batch_size = 0;
	MooseFsString<uint8_t> name_dst;

	if (header.type == CLTOMA_FUSE_SNAPSHOT) {
		deserializeAllMooseFsPacketDataNoHeader(data, header.length,
				msgid, inode, inode_dst, name_dst, uid, gid, canoverwrite);
		job_id = fs_reserve_job_id();
	} else if (header.type == LIZ_CLTOMA_FUSE_SNAPSHOT) {
		cltoma::snapshot::deserialize(data, header.length, msgid, job_id, inode,
		                              inode_dst, name_dst, uid, gid, canoverwrite,
		                              ignore_missing_src, initial_batch_size);
	} else {
		throw IncorrectDeserializationException(
				"Unknown packet type for matoclserv_fuse_snapshot: " +
				std::to_string(header.type));
	}
	status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);
		status = fs_snapshot(context, inode, inode_dst, HString(std::move(name_dst)),
		                     canoverwrite, ignore_missing_src, initial_batch_size,
		                     std::bind(matoclserv_fuse_snapshot_wake_up, header.type,
		                     eptr->sesdata->sessionid, msgid, std::placeholders::_1), job_id);
	}
	if (status != LIZARDFS_ERROR_WAITING) {
		matoclserv_fuse_snapshot_wake_up(header.type, eptr->sesdata->sessionid, msgid, status);
	}
}

void matoclserv_fuse_getdirstats_old(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inodes,files,dirs,chunks;
	uint64_t leng,size,rsize;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_GETDIRSTATS - wrong size (%" PRIu32 "/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_get_dir_stats(matoclserv_get_context(eptr),inode,&inodes,&dirs,&files,&chunks,&leng,&size,&rsize);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETDIRSTATS,(status!=LIZARDFS_STATUS_OK)?5:60);
	put32bit(&ptr,msgid);
	if (status!=LIZARDFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,inodes);
		put32bit(&ptr,dirs);
		put32bit(&ptr,files);
		put32bit(&ptr,0);
		put32bit(&ptr,0);
		put32bit(&ptr,chunks);
		put32bit(&ptr,0);
		put32bit(&ptr,0);
		put64bit(&ptr,leng);
		put64bit(&ptr,size);
		put64bit(&ptr,rsize);
	}
}

void matoclserv_fuse_getdirstats(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inodes,files,dirs,chunks;
	uint64_t leng,size,rsize;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_GETDIRSTATS - wrong size (%" PRIu32 "/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_get_dir_stats(matoclserv_get_context(eptr),inode,&inodes,&dirs,&files,&chunks,&leng,&size,&rsize);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETDIRSTATS,(status!=LIZARDFS_STATUS_OK)?5:44);
	put32bit(&ptr,msgid);
	if (status!=LIZARDFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,inodes);
		put32bit(&ptr,dirs);
		put32bit(&ptr,files);
		put32bit(&ptr,chunks);
		put64bit(&ptr,leng);
		put64bit(&ptr,size);
		put64bit(&ptr,rsize);
	}
}

void matoclserv_fuse_gettrash(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint32_t dleng;
	if (length!=4) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_GETTRASH - wrong size (%" PRIu32 "/4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	status = fs_readtrash_size(eptr->sesdata->rootinode,eptr->sesdata->sesflags,&dleng);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETTRASH,(status!=LIZARDFS_STATUS_OK)?5:(4+dleng));
	put32bit(&ptr,msgid);
	if (status!=LIZARDFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		fs_readtrash_data(eptr->sesdata->rootinode,eptr->sesdata->sesflags,ptr);
	}
}

void matoclserv_fuse_gettrash(matoclserventry *eptr, const PacketHeader &header, const uint8_t *data) {
	uint32_t off, max_entries, msg_id;
	cltoma::fuseGetTrash::deserialize(data, header.length, msg_id, off, max_entries);
	std::vector<NamedInodeEntry> entries;
	fs_readtrash(off, std::min<uint32_t>(max_entries, matocl::fuseGetDir::kMaxNumberOfDirectoryEntries), entries);
	matoclserv_createpacket(eptr, matocl::fuseGetTrash::build(msg_id, entries));
}

void matoclserv_fuse_getdetachedattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	Attributes attr;
	uint32_t msgid;
	uint8_t dtype;
	uint8_t *ptr;
	uint8_t status;
	if (length<8 || length>9) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_GETDETACHEDATTR - wrong size (%" PRIu32 "/8,9)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (length==9) {
		dtype = get8bit(&data);
	} else {
		dtype = DTYPE_UNKNOWN;
	}
	status = fs_getdetachedattr(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,attr,dtype);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETDETACHEDATTR,(status!=LIZARDFS_STATUS_OK)?5:39);
	put32bit(&ptr,msgid);
	if (status!=LIZARDFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		memcpy(ptr, attr.data(), attr.size());
	}
}

void matoclserv_fuse_gettrashpath(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	std::string path;
	if (length != 8) {
		lzfs_pretty_syslog(LOG_NOTICE, "CLTOMA_FUSE_GETTRASHPATH - wrong size (%" PRIu32 "/8)", length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_gettrashpath(eptr->sesdata->rootinode, eptr->sesdata->sesflags, inode, path);
	ptr = matoclserv_createpacket(eptr, MATOCL_FUSE_GETTRASHPATH,
	                              (status != LIZARDFS_STATUS_OK) ? 5 : 8 + path.length() + 1);
	put32bit(&ptr, msgid);
	if (status != LIZARDFS_STATUS_OK) {
		put8bit(&ptr, status);
	} else {
		put32bit(&ptr, path.length() + 1);
		if (path.length() > 0) {
			memcpy(ptr, path.c_str(), path.length());
		}
		ptr[path.length()] = 0;
	}
}

void matoclserv_fuse_settrashpath(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	const uint8_t *path;
	uint32_t pleng;
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (length<12) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_SETTRASHPATH - wrong size (%" PRIu32 "/>=12)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	pleng = get32bit(&data);
	if (length!=12+pleng) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_SETTRASHPATH - wrong size (%" PRIu32 "/%" PRIu32 ")",length,12+pleng);
		eptr->mode = KILL;
		return;
	}
	path = data;
	data += pleng;
	while (pleng>0 && path[pleng-1]==0) {
		pleng--;
	}
	status = fs_settrashpath(matoclserv_get_context(eptr), inode, std::string((char*)path, pleng));
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SETTRASHPATH,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_undel(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (length!=8) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_UNDEL - wrong size (%" PRIu32 "/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_undel(matoclserv_get_context(eptr), inode);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_UNDEL,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_purge(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_PURGE - wrong size (%" PRIu32 "/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_purge(matoclserv_get_context(eptr), inode);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_PURGE,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}


void matoclserv_fuse_getreserved(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint32_t dleng;
	if (length!=4) {
		lzfs_pretty_syslog(LOG_NOTICE,"CLTOMA_FUSE_GETRESERVED - wrong size (%" PRIu32 "/4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	status = fs_readreserved_size(eptr->sesdata->rootinode,eptr->sesdata->sesflags,&dleng);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETRESERVED,(status!=LIZARDFS_STATUS_OK)?5:(4+dleng));
	put32bit(&ptr,msgid);
	if (status!=LIZARDFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		fs_readreserved_data(eptr->sesdata->rootinode,eptr->sesdata->sesflags,ptr);
	}
}

void matoclserv_fuse_getreserved(matoclserventry *eptr, const PacketHeader &header, const uint8_t *data) {
	uint32_t off, max_entries, msg_id;
	cltoma::fuseGetReserved::deserialize(data, header.length, msg_id, off, max_entries);
	std::vector<NamedInodeEntry> entries;
	fs_readreserved(off, std::min<uint32_t>(max_entries, matocl::fuseGetDir::kMaxNumberOfDirectoryEntries), entries);
	matoclserv_createpacket(eptr, matocl::fuseGetReserved::build(msg_id, entries));
}

void matoclserv_fuse_deleteacl(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t messageId, inode, uid, gid;
	AclType type;
	cltoma::fuseDeleteAcl::deserialize(data, length, messageId, inode, uid, gid, type);

	uint8_t status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);
		status = fs_deleteacl(context, inode, type);
	}
	matoclserv_createpacket(eptr, matocl::fuseDeleteAcl::build(messageId, status));
}

void matoclserv_fuse_getacl(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t messageId, inode, uid, gid;
	AclType type;
	cltoma::fuseGetAcl::deserialize(data, length, messageId, inode, uid, gid, type);
	lzfs_silent_syslog(LOG_DEBUG, "master.cltoma_fuse_getacl: %u", inode);

	MessageBuffer reply;
	RichACL acl;

	uint8_t status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);
		status = fs_getacl(context, inode, acl);
	}
	if (status == LIZARDFS_STATUS_OK) {
		if (eptr->version >= kRichACLVersion) {
			FSNode *node = fsnodes_id_to_node(inode);
			uint32_t owner_id = node ? node->uid : RichACL::Ace::kInvalidId;
			matocl::fuseGetAcl::serialize(reply, messageId, owner_id, acl);
		} else {
			std::pair<bool, AccessControlList> posix_acl;
			if (type == AclType::kDefault) {
				posix_acl = acl.convertToDefaultPosixACL();
			} else {
				// default behavior for unknown acl type.
				posix_acl = acl.convertToPosixACL();
			}
			if (posix_acl.first) {
				if (eptr->version >= kACL11Version) {
					matocl::fuseGetAcl::serialize(reply, messageId, posix_acl.second);
				} else {
					legacy::AccessControlList legacy_acl = posix_acl.second;
					matocl::fuseGetAcl::serialize(reply, messageId, legacy_acl);
				}
			} else {
				status = LIZARDFS_ERROR_ENOATTR;
			}
		}
	}
	if (status != LIZARDFS_STATUS_OK) {
		matocl::fuseGetAcl::serialize(reply, messageId, status);
	}
	matoclserv_createpacket(eptr, std::move(reply));
}

static void matoclserv_lock_wake_up(uint32_t sessionid, uint32_t messageId,
		lzfs_locks::Type type) {
	matoclserventry *eptr;
	MessageBuffer reply;

	eptr = matoclserv_find_connection(sessionid);

	if (eptr == nullptr) {
		return;
	}

	switch (type) {
	case lzfs_locks::Type::kFlock:
		matocl::fuseFlock::serialize(reply, messageId, LIZARDFS_STATUS_OK);
		break;
	case lzfs_locks::Type::kPosix:
		matocl::fuseSetlk::serialize(reply, messageId, LIZARDFS_STATUS_OK);
		break;
	default:
		lzfs_pretty_syslog(LOG_ERR, "Incorrect lock type passed for lock wakeup: %u", (unsigned)type);
		return;
	}

	matoclserv_createpacket(eptr, std::move(reply));
}

static void matoclserv_lock_wake_up(std::vector<FileLocks::Owner> &owners, lzfs_locks::Type type) {
	for (auto owner : owners) {
		matoclserv_lock_wake_up(owner.sessionid, owner.msgid, type);
	}
}

void matoclserv_fuse_flock(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	FsContext context = FsContext::getForMaster(eventloop_time());
	uint32_t messageId;
	uint32_t inode;
	uint64_t owner;

	uint32_t requestId;
	uint16_t op;
	MessageBuffer reply;
	PacketVersion version;
	uint8_t status;

	bool nonblocking = false;

	deserializePacketVersionNoHeader(data, length, version);

	if (version != 0) {
		lzfs_pretty_syslog(LOG_ERR, "flock wrong message version\n");
		return;
	}
	cltoma::fuseFlock::deserialize(data, length, messageId, inode, owner, requestId, op);

	if (op & lzfs_locks::kNonblock) {
		nonblocking = true;
		op &= ~lzfs_locks::kNonblock;
	}

	std::vector<FileLocks::Owner> applied;
	status = fs_flock_op(context, inode, owner, eptr->sesdata->sessionid, requestId, messageId,
			op, nonblocking, applied);

	matoclserv_lock_wake_up(applied, lzfs_locks::Type::kFlock);

	// If it was a release request, do not respond
	if (op == lzfs_locks::kRelease) {
		return;
	}

	// Do not respond only if operation is blocking and status is WAITING
	if (nonblocking || status != LIZARDFS_ERROR_WAITING) {
		matocl::fuseFlock::serialize(reply, messageId, status);
		matoclserv_createpacket(eptr, std::move(reply));
	}
}

void matoclserv_fuse_getlk(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	FsContext context = FsContext::getForMaster(eventloop_time());
	uint32_t message_id;
	uint32_t inode;
	uint64_t owner;

	lzfs_locks::FlockWrapper lock_info;
	uint8_t status;
	uint64_t lock_end;

	cltoma::fuseGetlk::deserialize(data, length, message_id, inode, owner, lock_info);

	if (lock_info.l_start < 0 || lock_info.l_len < 0) {
		matoclserv_createpacket(eptr, matocl::fuseGetlk::build(message_id, LIZARDFS_ERROR_EINVAL));
		return;
	}

	// Standard states that lock of length 0 is a lock till EOF
	if (lock_info.l_len == 0) {
		lock_end = std::numeric_limits<uint64_t>::max();
	} else {
		lock_end = (uint64_t)lock_info.l_start + (uint64_t)lock_info.l_len;
	}

	status = fs_posixlock_probe(context, inode, lock_info.l_start, lock_end, owner,
			eptr->sesdata->sessionid, 0, message_id, lock_info.l_type, lock_info);

	// Standard states that lock of length 0 is a lock till EOF
	if (lock_info.l_len == std::numeric_limits<int64_t>::max()) {
		lock_info.l_len = 0;
	}

	if (status == LIZARDFS_ERROR_WAITING || status == LIZARDFS_STATUS_OK) {
		matoclserv_createpacket(eptr, matocl::fuseGetlk::build(message_id, lock_info));
	} else {
		matoclserv_createpacket(eptr, matocl::fuseGetlk::build(message_id, status));
	}
}

void matoclserv_fuse_setlk(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	FsContext context = FsContext::getForMaster(eventloop_time());
	uint32_t message_id;
	uint32_t inode;
	uint64_t owner;

	uint32_t request_id;
	uint16_t op;
	PacketVersion version;
	uint8_t status;
	lzfs_locks::FlockWrapper lock_info;
	uint64_t lock_end;

	bool nonblocking = false;
	deserializePacketVersionNoHeader(data, length, version);

	cltoma::fuseSetlk::deserialize(data, length, message_id, inode, owner, request_id, lock_info);

	if (lock_info.l_start < 0 || lock_info.l_len < 0) {
		matoclserv_createpacket(eptr, matocl::fuseSetlk::build(message_id, LIZARDFS_ERROR_EINVAL));
		return;
	}

	op = lock_info.l_type;

	if (op & lzfs_locks::kNonblock) {
		nonblocking = true;
		op &= ~lzfs_locks::kNonblock;
	}

	// Standard states that lock of length 0 is a lock till EOF
	if (lock_info.l_len == 0) {
		lock_end = std::numeric_limits<uint64_t>::max();
	} else {
		lock_end = (uint64_t)lock_info.l_start + (uint64_t)lock_info.l_len;
	}

	std::vector<FileLocks::Owner> applied;
	status = fs_posixlock_op(context, inode, lock_info.l_start, lock_end,
			owner, eptr->sesdata->sessionid, request_id, message_id, op, nonblocking, applied);

	matoclserv_lock_wake_up(applied, lzfs_locks::Type::kPosix);

	// If it was a release request, do not respond
	if (op == lzfs_locks::kRelease) {
		return;
	}

	// Do not respond only if operation is blocking and status is WAITING
	if (nonblocking || status != LIZARDFS_ERROR_WAITING) {
		matoclserv_createpacket(eptr, matocl::fuseSetlk::build(message_id, status));
	}
}

void matoclserv_list_defective_files(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	static const uint64_t kMaxNumberOfDefectiveEntries = 64 * 1024 * 1024;
	uint8_t flags;
	uint64_t entry_index, number_of_entries;
	cltoma::listDefectiveFiles::deserialize(data, length, flags, entry_index, number_of_entries);
	number_of_entries = std::min(number_of_entries, kMaxNumberOfDefectiveEntries);
	std::vector<DefectiveFileInfo> files_info = fs_get_defective_nodes_info(flags, number_of_entries, entry_index);
	matoclserv_createpacket(eptr, matocl::listDefectiveFiles::build(entry_index, files_info));
}

void matoclserv_manage_locks_list(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	FsContext context = FsContext::getForMaster(eventloop_time());
	uint32_t inode;
	lzfs_locks::Type type;
	bool pending;
	uint64_t start;
	uint64_t max;
	PacketVersion version;
	std::vector<lzfs_locks::Info> locks;
	int status;

	if (eptr->registered != ClientState::kAdmin) {
		lzfs_pretty_syslog(LOG_NOTICE, "Listing file locks is available only for registered admins");
		eptr->mode = KILL;
		return;
	}

	deserializePacketVersionNoHeader(data, length, version);

	if (version == cltoma::manageLocksList::kAll) {
		cltoma::manageLocksList::deserialize(data, length, type, pending, start, max);
		max = std::min(max, (uint64_t)LIZ_CLTOMA_MANAGE_LOCKS_LIST_LIMIT);
		status = fs_locks_list_all(context, (uint8_t)type, pending, start, max, locks);
	} else if (version == cltoma::manageLocksList::kInode) {
		cltoma::manageLocksList::deserialize(data, length, inode, type, pending, start, max);
		max = std::min(max, (uint64_t)LIZ_CLTOMA_MANAGE_LOCKS_LIST_LIMIT);
		status = fs_locks_list_inode(context, (uint8_t)type, pending, inode, start, max, locks);
	} else {
		throw IncorrectDeserializationException(
				"Unknown LIZ_CLTOMA_MANAGE_LOCKS_LIST version: " + std::to_string(version));
	}

	if (status != LIZARDFS_STATUS_OK) {
		lzfs_pretty_syslog(LOG_WARNING, "Master received invalid lock type %" PRIu8
		                                " from in LIZ_CLTOMA_MANAGE_LOCKS_LIST packet",
		                   (uint8_t)type);
	}

	MessageBuffer reply;
	matocl::manageLocksList::serialize(reply, locks);
	matoclserv_createpacket(eptr, std::move(reply));
}

void matoclserv_manage_locks_unlock(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	FsContext context = FsContext::getForMaster(eventloop_time());
	uint32_t inode;
	uint64_t owner;
	uint32_t sessionid;
	lzfs_locks::Type type;
	uint64_t start;
	uint64_t end;
	PacketVersion version;
	uint8_t status = LIZARDFS_STATUS_OK;
	std::vector<FileLocks::Owner> flocks_applied, posix_applied;

	if (eptr->registered != ClientState::kAdmin) {
		lzfs_pretty_syslog(LOG_NOTICE, "Removing file locks is available only for registered admins");
		eptr->mode = KILL;
		return;
	}

	deserializePacketVersionNoHeader(data, length, version);

	if (version == cltoma::manageLocksUnlock::kSingle) {
		cltoma::manageLocksUnlock::deserialize(data, length, type, inode, sessionid, owner, start,
		                                       end);
		// Passing a 0 as lock's end is equivalent to passing 'till EOF'
		if (end == 0) {
			end = std::numeric_limits<decltype(end)>::max();
		}
		if (type == lzfs_locks::Type::kAll || type == lzfs_locks::Type::kFlock) {
			status = fs_flock_op(context, inode, owner, sessionid, 0, 0, lzfs_locks::kUnlock, true,
			                     flocks_applied);
		}
		if (status == LIZARDFS_STATUS_OK &&
		    (type == lzfs_locks::Type::kAll || type == lzfs_locks::Type::kPosix)) {
			status = fs_posixlock_op(context, inode, start, end, owner, sessionid, 0, 0,
			                         lzfs_locks::kUnlock, true, posix_applied);
		}
	} else if (version == cltoma::manageLocksUnlock::kInode) {
		cltoma::manageLocksUnlock::deserialize(data, length, type, inode);
		if (type == lzfs_locks::Type::kAll || type == lzfs_locks::Type::kFlock) {
			status = fs_locks_unlock_inode(context, (uint8_t)lzfs_locks::Type::kFlock, inode,
			                               flocks_applied);
		}
		if (status == LIZARDFS_STATUS_OK &&
		    (type == lzfs_locks::Type::kAll || type == lzfs_locks::Type::kPosix)) {
			status = fs_locks_unlock_inode(context, (uint8_t)lzfs_locks::Type::kPosix, inode,
			                               posix_applied);
		}
	} else {
		throw IncorrectDeserializationException("Unknown LIZ_CLTOMA_MANAGE_LOCKS_UNLOCK version: " +
		                                        std::to_string(version));
	}

	for (auto sessionAndMsg : flocks_applied) {
		matoclserv_lock_wake_up(sessionAndMsg.sessionid, sessionAndMsg.msgid,
		                        lzfs_locks::Type::kFlock);
	}
	for (auto sessionAndMsg : posix_applied) {
		matoclserv_lock_wake_up(sessionAndMsg.sessionid, sessionAndMsg.msgid,
		                        lzfs_locks::Type::kPosix);
	}

	MessageBuffer reply;
	matocl::manageLocksUnlock::serialize(reply, status);
	matoclserv_createpacket(eptr, std::move(reply));
}

void matoclserv_list_tasks(matoclserventry *eptr) {
	std::vector<JobInfo> jobs_info = fs_get_current_tasks_info();
	matoclserv_createpacket(eptr, matocl::listTasks::build(jobs_info));
}

void matoclserv_stop_task(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t job_id, msgid;
	uint8_t status;
	cltoma::stopTask::deserialize(data, length, msgid, job_id);
	status = fs_cancel_job(job_id);
	matoclserv_createpacket(eptr, matocl::stopTask::build(msgid, status));
}

void matoclserv_fuse_locks_interrupt(matoclserventry *eptr, const uint8_t *data, uint32_t length,
				     uint8_t type) {
	FsContext context = FsContext::getForMaster(eventloop_time());
	uint32_t messageId;
	lzfs_locks::InterruptData interruptData;

	PacketVersion version;
	deserializePacketVersionNoHeader(data, length, version);

	if (version != 0) {
		lzfs_pretty_syslog(LOG_ERR, "fuse_flock_interrupt wrong message version\n");
		return;
	}

	cltoma::fuseFlock::deserialize(data, length, messageId, interruptData);

	// we do not reply, so there is not need for checking status of this fs_operation
	fs_locks_remove_pending(context, type, interruptData.owner,
			   eptr->sesdata->sessionid, interruptData.ino, interruptData.reqid);
}

void matoclserv_update_credentials(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t messageId, index;
	FsContext::GroupsContainer gids;

	cltoma::updateCredentials::deserialize(data, length, messageId, index, gids);

	assert(eptr->sesdata);

	auto it = eptr->sesdata->group_cache.find(index);
	if (it != eptr->sesdata->group_cache.end()) {
		it->second.clear();
		it->second.insert(it->second.end(), gids.begin(), gids.end());
	} else {
		FsContext::GroupsContainer tmp(gids.begin(), gids.end());
		eptr->sesdata->group_cache.insert(std::move(index), std::move(tmp));
	}

	matoclserv_createpacket(eptr, matocl::updateCredentials::build(messageId, LIZARDFS_STATUS_OK));
}

void matoclserv_fuse_setacl(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t messageId, inode, uid, gid;
	AclType type = AclType::kRichACL;
	RichACL rich_acl;
	AccessControlList posix_acl;
	bool use_posix = false;

	PacketVersion version;
	deserializePacketVersionNoHeader(data, length, version);

	if (version == cltoma::fuseSetAcl::kLegacyACL) {
		legacy::AccessControlList legacy_acl;
		cltoma::fuseSetAcl::deserialize(data, length, messageId, inode, uid, gid, type, legacy_acl);
		use_posix = true;
		posix_acl = (AccessControlList)legacy_acl;
	} else if (version == cltoma::fuseSetAcl::kPosixACL) {
		use_posix = true;
		cltoma::fuseSetAcl::deserialize(data, length, messageId, inode, uid, gid, type, posix_acl);
	} else if (version == cltoma::fuseSetAcl::kRichACL) {
		cltoma::fuseSetAcl::deserialize(data, length, messageId, inode, uid, gid, rich_acl);
	} else {
		lzfs_pretty_syslog(LOG_WARNING, "LIZ_CLTOMA_FUSE_SET_ACL: unknown packet version");
		eptr->mode = KILL;
		return;
	}

	uint8_t status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);
		if (use_posix) {
			status = fs_setacl(context, inode, type, posix_acl);
		} else {
			status = fs_setacl(context, inode, rich_acl);
		}
	}
	matoclserv_createpacket(eptr, matocl::fuseSetAcl::build(messageId, status));
}

void matoclserv_fuse_setquota(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t messageId, uid, gid;
	std::vector<QuotaEntry> entries;
	cltoma::fuseSetQuota::deserialize(data, length, messageId, uid, gid, entries);

	uint8_t status = matoclserv_check_group_cache(eptr, gid);
	if (status == LIZARDFS_STATUS_OK) {
		FsContext context = matoclserv_get_context(eptr, uid, gid);
		status = fs_quota_set(context, entries);
	}
	MessageBuffer reply;
	matocl::fuseSetQuota::serialize(reply, messageId, status);
	matoclserv_createpacket(eptr, std::move(reply));
}

void matoclserv_fuse_getquota(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t version, messageId, uid, gid;
	std::vector<QuotaEntry> results;
	std::vector<std::string> info;
	uint8_t status;
	deserializePacketVersionNoHeader(data, length, version);
	if (version == cltoma::fuseGetQuota::kAllLimits) {
		cltoma::fuseGetQuota::deserialize(data, length, messageId, uid, gid);
		status = matoclserv_check_group_cache(eptr, gid);
		if (status == LIZARDFS_STATUS_OK) {
			FsContext context = matoclserv_get_context(eptr, uid, gid);
			status = fs_quota_get_all(context, results);
		}
	} else if (version == cltoma::fuseGetQuota::kSelectedLimits) {
		std::vector<QuotaOwner> owners;
		cltoma::fuseGetQuota::deserialize(data, length, messageId, uid, gid, owners);
		status = matoclserv_check_group_cache(eptr, gid);
		if (status == LIZARDFS_STATUS_OK) {
			FsContext context = matoclserv_get_context(eptr, uid, gid);
			status = fs_quota_get(context, owners, results);
		}
	} else {
		throw IncorrectDeserializationException(
				"Unknown LIZ_CLTOMA_FUSE_GET_QUOTA version: " + std::to_string(version));
	}
	MessageBuffer reply;
	if (status == LIZARDFS_STATUS_OK) {
		status = fs_quota_get_info(matoclserv_get_context(eptr), results, info);
	}
	if (status == LIZARDFS_STATUS_OK) {
		matocl::fuseGetQuota::serialize(reply, messageId, results, info);
	} else {
		matocl::fuseGetQuota::serialize(reply, messageId, status);
	}
	matoclserv_createpacket(eptr, std::move(reply));
}

void matoclserv_iolimit(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t msgid;
	uint32_t configVersion;
	std::string groupId;
	uint64_t requestedBytes;
	cltoma::iolimit::deserialize(data, length, msgid, configVersion, groupId, requestedBytes);
	uint64_t grantedBytes;
	if (configVersion != gIoLimitsConfigId) {
		grantedBytes = 0;
	} else {
		try {
			grantedBytes = gIoLimitsDatabase.request(
					SteadyClock::now(), groupId, requestedBytes);
		} catch (IoLimitsDatabase::InvalidGroupIdException&) {
			lzfs_pretty_syslog(LOG_NOTICE, "LIZ_CLTOMA_IOLIMIT: Invalid group: %s", groupId.c_str());
			grantedBytes = 0;
		}
	}
	MessageBuffer reply;
	matocl::iolimit::serialize(reply, msgid, configVersion, groupId, grantedBytes);
	matoclserv_createpacket(eptr, std::move(reply));
}

void matoclserv_hostname(matoclserventry* eptr, const uint8_t* data, uint32_t length) {
	cltoma::hostname::deserialize(data, length);
	char hostname[257];
	memset(hostname, 0, 257);
	// Use 1 byte less then the array has in order to ensure that the name is null terminated:
	gethostname(hostname, 256);
	matoclserv_createpacket(eptr, matocl::hostname::build(std::string(hostname)));
}

void matoclserv_admin_register(matoclserventry* eptr, const uint8_t* data, uint32_t length) {
	cltoma::adminRegister::deserialize(data, length);
	if (!eptr->adminChallenge) {
		eptr->adminChallenge.reset(new LizMatoclAdminRegisterChallengeData());
		auto& array = *eptr->adminChallenge;
		for (uint32_t i = 0; i < array.size(); ++i) {
			array[i] = rnd<uint8_t>();
		}
		matoclserv_createpacket(eptr, matocl::adminRegisterChallenge::build(array));
	} else {
		lzfs_pretty_syslog(LOG_NOTICE, "LIZ_CLTOMA_ADMIN_REGISTER_CHALLENGE: retry not allowed");
		eptr->mode = KILL;
	}
}

void matoclserv_admin_register_response(matoclserventry* eptr, const uint8_t* data,
		uint32_t length) {
	LizCltomaAdminRegisterResponseData receivedDigest;
	cltoma::adminRegisterResponse::deserialize(data, length, receivedDigest);
	if (eptr->adminChallenge) {
		std::string password = cfg_getstring("ADMIN_PASSWORD", "");
		if (password == "") {
			matoclserv_createpacket(eptr, matocl::adminRegisterResponse::build(LIZARDFS_ERROR_EPERM));
			lzfs_pretty_syslog(LOG_WARNING, "admin access disabled");
			return;
		}
		auto digest = md5_challenge_response(*eptr->adminChallenge, password);
		if (receivedDigest == digest) {
			matoclserv_createpacket(eptr, matocl::adminRegisterResponse::build(LIZARDFS_STATUS_OK));
			eptr->registered = ClientState::kAdmin;
		} else {
			matoclserv_createpacket(eptr, matocl::adminRegisterResponse::build(LIZARDFS_ERROR_BADPASSWORD));
			lzfs_pretty_syslog(LOG_WARNING, "admin authentication error");
		}
		eptr->adminChallenge.reset();
	} else {
		lzfs_pretty_syslog(LOG_NOTICE,
				"LIZ_CLTOMA_ADMIN_REGISTER_RESPONSE: response without previous challenge");
		eptr->mode = KILL;
	}
}

void matoclserv_admin_become_master(matoclserventry* eptr, const uint8_t* data, uint32_t length) {
	cltoma::adminBecomeMaster::deserialize(data, length);
	if (eptr->registered == ClientState::kAdmin) {
		bool succ = metadataserver::promoteAutoToMaster();
		uint8_t status = succ ? LIZARDFS_STATUS_OK : LIZARDFS_ERROR_NOTPOSSIBLE;
		matoclserv_createpacket(eptr, matocl::adminBecomeMaster::build(status));
	} else {
		lzfs_pretty_syslog(LOG_NOTICE,
				"LIZ_CLTOMA_ADMIN_BECOME_MASTER: available only for registered admins");
		eptr->mode = KILL;
	}
}

void matoclserv_admin_stop_without_metadata_dump(
			matoclserventry* eptr, const uint8_t* data, uint32_t length) {
	cltoma::adminStopWithoutMetadataDump::deserialize(data, length);
	if (eptr->registered == ClientState::kAdmin) {
		if (metadataserver::isMaster()) {
			if (matomlserv_shadows_count() == 0){
				lzfs_pretty_syslog(LOG_WARNING, "LIZ_CLTOMA_ADMIN_STOP_WITHOUT_METADATA_DUMP: Trying to stop"
						" master server with disabled metadata dump when no shadow servers are "
						"connected.");
				matoclserv_createpacket(eptr, matocl::adminStopWithoutMetadataDump::build(EPERM));
			} else {
				fs_disable_metadata_dump_on_exit();
				uint8_t status = eventloop_want_to_terminate();
				if (status == LIZARDFS_STATUS_OK) {
					eptr->adminTask = AdminTask::kTerminate;
				} else {
					matoclserv_createpacket(
							eptr, matocl::adminStopWithoutMetadataDump::build(status));
				}
			}
		} else { // not Master
			fs_disable_metadata_dump_on_exit();
			uint8_t status = eventloop_want_to_terminate();
			matoclserv_createpacket(eptr, matocl::adminStopWithoutMetadataDump::build(status));
		}
	} else {
		lzfs_pretty_syslog(LOG_NOTICE,
				"LIZ_CLTOMA_ADMIN_STOP_WITHOUT_METADATA_DUMP:"
				" available only for registered admins");
		eptr->mode = KILL;
	}
}

void matoclserv_admin_reload(matoclserventry* eptr, const uint8_t* data, uint32_t length) {
	cltoma::adminReload::deserialize(data, length);
	if (eptr->registered == ClientState::kAdmin) {
		eptr->adminTask = AdminTask::kReload; // mark, that this admin waits for response
		eventloop_want_to_reload();
		lzfs_pretty_syslog(LOG_NOTICE, "reload of the config file requested using lizardfs-admin by %s",
				ipToString(eptr->peerip).c_str());
	} else {
		lzfs_pretty_syslog(LOG_NOTICE, "LIZ_CLTOMA_ADMIN_RELOAD: available only for registered admins");
		eptr->mode = KILL;
	}
}

void matoclserv_admin_save_metadata(matoclserventry* eptr, const uint8_t* data, uint32_t length) {
	bool asynchronous;
	cltoma::adminSaveMetadata::deserialize(data, length, asynchronous);
	if (eptr->registered == ClientState::kAdmin) {
		lzfs_pretty_syslog(LOG_NOTICE, "saving metadata image requested using lizardfs-admin by %s",
				ipToString(eptr->peerip).c_str());
		uint8_t status = fs_storeall(MetadataDumper::DumpType::kBackgroundDump);
		if (status != LIZARDFS_STATUS_OK || asynchronous) {
			matoclserv_createpacket(eptr, matocl::adminSaveMetadata::build(status));
		} else {
			// Mark the client; we will reply after metadata save process is finished
			eptr->adminTask = AdminTask::kSaveMetadata;
		}
	} else {
		lzfs_pretty_syslog(LOG_NOTICE, "LIZ_CLTOMA_ADMIN_SAVE_METADATA: available only for registered admins");
		eptr->mode = KILL;
	}
}

void matoclserv_broadcast_metadata_saved(uint8_t status) {
	if (exiting) {
		return;
	}
	for (matoclserventry* eptr = matoclservhead; eptr != nullptr; eptr = eptr->next) {
		if (eptr->adminTask == AdminTask::kSaveMetadata) {
			matoclserv_createpacket(eptr, matocl::adminSaveMetadata::build(status));
			eptr->adminTask = AdminTask::kNone;
		}
	}
}

void matoclserv_admin_recalculate_metadata_checksum(matoclserventry* eptr,
		const uint8_t* data, uint32_t length) {
	bool asynchronous;
	cltoma::adminRecalculateMetadataChecksum::deserialize(data, length, asynchronous);
	if (eptr->registered == ClientState::kAdmin) {
		lzfs_pretty_syslog(LOG_NOTICE, "metadata checksum recalculation requested using lizardfs-admin by %s",
					ipToString(eptr->peerip).c_str());
		uint8_t status = fs_start_checksum_recalculation();
		if (status != LIZARDFS_STATUS_OK || asynchronous) {
			matoclserv_createpacket(eptr, matocl::adminRecalculateMetadataChecksum::build(status));
		} else {
			// Mark the client; we will reply after checksum of metadata is recalculated
			eptr->adminTask = AdminTask::kRecalculateChecksums;
		}
	} else {
		lzfs_pretty_syslog(LOG_NOTICE, "LIZ_CLTOMA_ADMIN_RECALCULATE_METADATA_CHECKSUM: "
				"available only for registered admins");
		eptr->mode = KILL;
	}
}

void matoclserv_broadcast_metadata_checksum_recalculated(uint8_t status) {
	if (exiting) {
		return;
	}
	for (matoclserventry* eptr = matoclservhead; eptr != nullptr; eptr = eptr->next) {
		if (eptr->adminTask == AdminTask::kRecalculateChecksums) {
			matoclserv_createpacket(eptr, matocl::adminRecalculateMetadataChecksum::build(status));
			eptr->adminTask = AdminTask::kNone;
		}
	}
}

void matocl_locks_release(const FsContext &context, uint32_t inode, uint32_t sessionid) {
	std::vector<FileLocks::Owner> applied;

	fs_locks_clear_session(context, (uint8_t)lzfs_locks::Type::kFlock, inode, sessionid, applied);
	for (auto candidate : applied) {
		matoclserv_lock_wake_up(candidate.sessionid, candidate.msgid, lzfs_locks::Type::kFlock);
	}

	applied.clear();
	fs_locks_clear_session(context, (uint8_t)lzfs_locks::Type::kPosix, inode, sessionid, applied);
	for (auto candidate : applied) {
		matoclserv_lock_wake_up(candidate.sessionid, candidate.msgid, lzfs_locks::Type::kPosix);
	}
}

void matocl_session_timedout(session *sesdata) {
	filelist *fl,*afl;
	fl=sesdata->openedfiles;
	FsContext context = FsContext::getForMaster(eventloop_time());
	while (fl) {
		afl = fl;
		fl=fl->next;
		fs_release(context, afl->inode, sesdata->sessionid);
		matocl_locks_release(context, afl->inode, sesdata->sessionid);
		free(afl);
	}
	sesdata->openedfiles=NULL;
	if (sesdata->info) {
		free(sesdata->info);
	}
}

void matocl_session_check(void) {
	session **sesdata,*asesdata;
	uint32_t now;

	now = eventloop_time();
	sesdata = &(sessionshead);
	while ((asesdata=*sesdata)) {
//              syslog(LOG_NOTICE,"session: %u ; nsocks: %u ; state: %u ; disconnected: %u",asesdata->sessionid,asesdata->nsocks,asesdata->newsession,asesdata->disconnected);
		if (asesdata->nsocks==0 && ((asesdata->newsession>1 && asesdata->disconnected<now) || (asesdata->newsession==1 && asesdata->disconnected+SessionSustainTime<now) || (asesdata->newsession==0 && asesdata->disconnected+7200<now))) {
//                      syslog(LOG_NOTICE,"remove session: %u",asesdata->sessionid);
			matocl_session_timedout(asesdata);
			*sesdata = asesdata->next;
			delete asesdata;
		} else {
			sesdata = &(asesdata->next);
		}
	}
}

void matocl_session_statsmove(void) {
	session *sesdata;
	for (sesdata = sessionshead ; sesdata ; sesdata=sesdata->next) {
		sesdata->lasthouropstats = sesdata->currentopstats;
		sesdata->currentopstats.fill(0);
	}
	matoclserv_store_sessions();
}

void matocl_beforedisconnect(matoclserventry *eptr) {
	chunklist *cl,*acl;
// unlock locked chunks
	cl=eptr->chunkdelayedops;
	while (cl) {
		acl = cl;
		cl=cl->next;
		if (acl->type == FUSE_TRUNCATE) {
			fs_end_setlength(acl->chunkid);
		}
		free(acl);
	}
	eptr->chunkdelayedops=NULL;
	if (eptr->sesdata) {
		if (eptr->sesdata->nsocks>0) {
			eptr->sesdata->nsocks--;
		}
		if (eptr->sesdata->nsocks==0) {
			eptr->sesdata->disconnected = eventloop_time();
		}
	}
}

void matoclserv_gotpacket(matoclserventry *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
	if (type==ANTOAN_NOP) {
		return;
	}
	if (type==ANTOAN_UNKNOWN_COMMAND) { // for future use
		return;
	}
	if (type==ANTOAN_BAD_COMMAND_SIZE) { // for future use
		return;
	}
	if (type==ANTOAN_PING) {
		matoclserv_ping(eptr,data,length);
		return;
	}
	try {
		if (!metadataserver::isMaster()) {     // shadow
			switch (type) {
				case LIZ_CLTOMA_METADATASERVER_STATUS:
					matoclserv_metadataserver_status(eptr, data, length);
					break;
				case LIZ_CLTOMA_HOSTNAME:
					matoclserv_hostname(eptr, data, length);
					break;
				case LIZ_CLTOMA_ADMIN_REGISTER_CHALLENGE:
					matoclserv_admin_register(eptr, data, length);
					break;
				case LIZ_CLTOMA_ADMIN_REGISTER_RESPONSE:
					matoclserv_admin_register_response(eptr, data, length);
					break;
				case LIZ_CLTOMA_ADMIN_BECOME_MASTER:
					matoclserv_admin_become_master(eptr, data, length);
					break;
				case LIZ_CLTOMA_ADMIN_STOP_WITHOUT_METADATA_DUMP:
					matoclserv_admin_stop_without_metadata_dump(eptr, data, length);
					break;
				case LIZ_CLTOMA_ADMIN_RELOAD:
					matoclserv_admin_reload(eptr, data, length);
					break;
				case LIZ_CLTOMA_ADMIN_SAVE_METADATA:
					matoclserv_admin_save_metadata(eptr, data, length);
					break;
				default:
					lzfs_pretty_syslog(LOG_NOTICE,"main master server module: got invalid message in shadow state (type:%" PRIu32 ")",type);
					eptr->mode = KILL;
			}
		} else if (eptr->registered == ClientState::kUnregistered
				|| eptr->registered == ClientState::kAdmin) { // beware that in this context sesdata is NULL
			switch (type) {
				case CLTOMA_FUSE_REGISTER:
					matoclserv_fuse_register(eptr,data,length);
					break;
				case CLTOMA_CSERV_LIST:
					matoclserv_cserv_list(eptr,data,length);
					break;
				case LIZ_CLTOMA_CSERV_LIST:
					matoclserv_liz_cserv_list(eptr, data, length);
					break;
				case CLTOMA_SESSION_LIST:
					matoclserv_session_list(eptr,data,length);
					break;
				case CLTOAN_CHART:
					matoclserv_chart(eptr,data,length);
					break;
				case CLTOAN_CHART_DATA:
					matoclserv_chart_data(eptr,data,length);
					break;
				case CLTOMA_INFO:
					matoclserv_info(eptr,data,length);
					break;
				case CLTOMA_FSTEST_INFO:
					matoclserv_fstest_info(eptr,data,length);
					break;
				case CLTOMA_CHUNKSTEST_INFO:
					matoclserv_chunkstest_info(eptr,data,length);
					break;
				case CLTOMA_CHUNKS_MATRIX:
					matoclserv_chunks_matrix(eptr,data,length);
					break;
				case CLTOMA_EXPORTS_INFO:
					matoclserv_exports_info(eptr,data,length);
					break;
				case CLTOMA_MLOG_LIST:
					matoclserv_mlog_list(eptr,data,length);
					break;
				case CLTOMA_CSSERV_REMOVESERV:
					matoclserv_cserv_removeserv(eptr,data,length);
					break;
				case LIZ_CLTOMA_IOLIMITS_STATUS:
					matoclserv_iolimits_status(eptr, data, length);
					break;
				case LIZ_CLTOMA_METADATASERVERS_LIST:
					matoclserv_metadataservers_list(eptr, data, length);
					break;
				case LIZ_CLTOMA_METADATASERVER_STATUS:
					matoclserv_metadataserver_status(eptr, data, length);
					break;
				case LIZ_CLTOMA_LIST_GOALS:
					matoclserv_list_goals(eptr);
					break;
				case LIZ_CLTOMA_CHUNKS_HEALTH:
					matoclserv_chunks_health(eptr, data, length);
					break;
				case LIZ_CLTOMA_HOSTNAME:
					matoclserv_hostname(eptr, data, length);
					break;
				case LIZ_CLTOMA_ADMIN_REGISTER_CHALLENGE:
					matoclserv_admin_register(eptr, data, length);
					break;
				case LIZ_CLTOMA_ADMIN_REGISTER_RESPONSE:
					matoclserv_admin_register_response(eptr, data, length);
					break;
				case LIZ_CLTOMA_ADMIN_STOP_WITHOUT_METADATA_DUMP:
					matoclserv_admin_stop_without_metadata_dump(eptr, data, length);
					break;
				case LIZ_CLTOMA_ADMIN_RELOAD:
					matoclserv_admin_reload(eptr, data, length);
					break;
				case LIZ_CLTOMA_ADMIN_SAVE_METADATA:
					matoclserv_admin_save_metadata(eptr, data, length);
					break;
				case LIZ_CLTOMA_ADMIN_RECALCULATE_METADATA_CHECKSUM:
					matoclserv_admin_recalculate_metadata_checksum(eptr, data, length);
					break;
				case LIZ_CLTOMA_LIST_TAPESERVERS:
					matoclserv_list_tapeservers(eptr, data, length);
					break;
				case LIZ_CLTOMA_LIST_DEFECTIVE_FILES:
					matoclserv_list_defective_files(eptr, data, length);
					break;
				case LIZ_CLTOMA_MANAGE_LOCKS_LIST:
					matoclserv_manage_locks_list(eptr,data,length);
					break;
				case LIZ_CLTOMA_MANAGE_LOCKS_UNLOCK:
					matoclserv_manage_locks_unlock(eptr,data,length);
					break;
				case LIZ_CLTOMA_LIST_TASKS:
					matoclserv_list_tasks(eptr);
					break;
				case LIZ_CLTOMA_STOP_TASK:
					matoclserv_stop_task(eptr, data, length);
					break;
				default:
					lzfs_pretty_syslog(LOG_NOTICE,"main master server module: got unknown message from unregistered (type:%" PRIu32 ")",type);
					eptr->mode=KILL;
			}
		} else if (eptr->registered == ClientState::kRegistered) {      // mounts and new tools
			if (eptr->sesdata==NULL) {
				lzfs_pretty_syslog(LOG_ERR,"registered connection without sesdata !!!");
				eptr->mode=KILL;
				return;
			}
			switch (type) {
				case CLTOMA_FUSE_REGISTER:
					matoclserv_fuse_register(eptr,data,length);
					break;
				case CLTOMA_FUSE_RESERVED_INODES:
					matoclserv_fuse_reserved_inodes(eptr,data,length);
					break;
				case CLTOMA_FUSE_STATFS:
					matoclserv_fuse_statfs(eptr,data,length);
					break;
				case CLTOMA_FUSE_ACCESS:
					matoclserv_fuse_access(eptr,data,length);
					break;
				case CLTOMA_FUSE_LOOKUP:
					matoclserv_fuse_lookup(eptr,data,length);
					break;
				case CLTOMA_FUSE_GETATTR:
					matoclserv_fuse_getattr(eptr,data,length);
					break;
				case CLTOMA_FUSE_SETATTR:
					matoclserv_fuse_setattr(eptr,data,length);
					break;
				case CLTOMA_FUSE_READLINK:
					matoclserv_fuse_readlink(eptr,data,length);
					break;
				case CLTOMA_FUSE_SYMLINK:
					matoclserv_fuse_symlink(eptr,data,length);
					break;
				case CLTOMA_FUSE_MKNOD:
				case LIZ_CLTOMA_FUSE_MKNOD:
					matoclserv_fuse_mknod(eptr, PacketHeader(type, length), data);
					break;
				case CLTOMA_FUSE_MKDIR:
				case LIZ_CLTOMA_FUSE_MKDIR:
					matoclserv_fuse_mkdir(eptr, PacketHeader(type, length), data);
					break;
				case CLTOMA_FUSE_UNLINK:
					matoclserv_fuse_unlink(eptr,data,length);
					break;
				case CLTOMA_FUSE_RMDIR:
					matoclserv_fuse_rmdir(eptr,data,length);
					break;
				case CLTOMA_FUSE_RENAME:
					matoclserv_fuse_rename(eptr,data,length);
					break;
				case CLTOMA_FUSE_LINK:
					matoclserv_fuse_link(eptr,data,length);
					break;
				case CLTOMA_FUSE_GETDIR:
					matoclserv_fuse_getdir(eptr,data,length);
					break;
				case LIZ_CLTOMA_FUSE_GETDIR:
					matoclserv_fuse_getdir(eptr, PacketHeader(type, length), data);
					break;
				case CLTOMA_FUSE_OPEN:
					matoclserv_fuse_open(eptr,data,length);
					break;
				case LIZ_CLTOMA_FUSE_READ_CHUNK:
				case CLTOMA_FUSE_READ_CHUNK:
					matoclserv_fuse_read_chunk(eptr, PacketHeader(type, length), data);
					break;
				case LIZ_CLTOMA_CHUNKS_INFO:
					matoclserv_chunks_info(eptr, data, length);
					break;
				case LIZ_CLTOMA_TAPE_INFO:
					matoclserv_tape_info(eptr, data, length);
					break;
				case LIZ_CLTOMA_FUSE_WRITE_CHUNK:
				case CLTOMA_FUSE_WRITE_CHUNK:
					matoclserv_fuse_write_chunk(eptr, PacketHeader(type, length), data);
					break;
				case LIZ_CLTOMA_FUSE_WRITE_CHUNK_END:
				case CLTOMA_FUSE_WRITE_CHUNK_END:
					matoclserv_fuse_write_chunk_end(eptr, PacketHeader(type, length), data);
					break;
					// fuse - meta
				case CLTOMA_FUSE_GETTRASH:
					matoclserv_fuse_gettrash(eptr,data,length);
					break;
				case LIZ_CLTOMA_FUSE_GETTRASH:
					matoclserv_fuse_gettrash(eptr, PacketHeader(type, length), data);
					break;
				case CLTOMA_FUSE_GETDETACHEDATTR:
					matoclserv_fuse_getdetachedattr(eptr,data,length);
					break;
				case CLTOMA_FUSE_GETTRASHPATH:
					matoclserv_fuse_gettrashpath(eptr,data,length);
					break;
				case CLTOMA_FUSE_SETTRASHPATH:
					matoclserv_fuse_settrashpath(eptr,data,length);
					break;
				case CLTOMA_FUSE_UNDEL:
					matoclserv_fuse_undel(eptr,data,length);
					break;
				case CLTOMA_FUSE_PURGE:
					matoclserv_fuse_purge(eptr,data,length);
					break;
				case CLTOMA_FUSE_GETRESERVED:
					matoclserv_fuse_getreserved(eptr,data,length);
					break;
				case LIZ_CLTOMA_FUSE_GETRESERVED:
					matoclserv_fuse_getreserved(eptr, PacketHeader(type, length), data);
					break;
				case CLTOMA_FUSE_CHECK:
					matoclserv_fuse_check(eptr,data,length);
					break;
				case CLTOMA_FUSE_GETTRASHTIME:
					matoclserv_fuse_gettrashtime(eptr,data,length);
					break;
				case CLTOMA_FUSE_SETTRASHTIME:
					matoclserv_fuse_settrashtime(eptr, PacketHeader(type, length), data);
					break;
				case CLTOMA_FUSE_GETGOAL:
				case LIZ_CLTOMA_FUSE_GETGOAL:
					matoclserv_fuse_getgoal(eptr, PacketHeader(type, length), data);
					break;
				case CLTOMA_FUSE_SETGOAL:
				case LIZ_CLTOMA_FUSE_SETGOAL:
					matoclserv_fuse_setgoal(eptr, PacketHeader(type, length), data);
					break;
				case CLTOMA_FUSE_APPEND:
					matoclserv_fuse_append(eptr,data,length);
					break;
				case CLTOMA_FUSE_GETDIRSTATS:
					matoclserv_fuse_getdirstats_old(eptr,data,length);
					break;
				case LIZ_CLTOMA_FUSE_TRUNCATE_END:
				case LIZ_CLTOMA_FUSE_TRUNCATE:
				case CLTOMA_FUSE_TRUNCATE:
					matoclserv_fuse_truncate(eptr, PacketHeader(type, length), data);
					break;
				case CLTOMA_FUSE_REPAIR:
					matoclserv_fuse_repair(eptr,data,length);
					break;
				case CLTOMA_FUSE_SNAPSHOT:
				case LIZ_CLTOMA_FUSE_SNAPSHOT:
					matoclserv_fuse_snapshot(eptr, PacketHeader(type, length), data);
					break;
				case CLTOMA_FUSE_GETEATTR:
					matoclserv_fuse_geteattr(eptr,data,length);
					break;
				case CLTOMA_FUSE_SETEATTR:
					matoclserv_fuse_seteattr(eptr,data,length);
					break;
				case LIZ_CLTOMA_FUSE_DELETE_ACL:
					matoclserv_fuse_deleteacl(eptr, data, length);
					break;
				case LIZ_CLTOMA_FUSE_GET_ACL:
					matoclserv_fuse_getacl(eptr, data, length);
					break;
				case LIZ_CLTOMA_FUSE_SET_ACL:
					matoclserv_fuse_setacl(eptr, data, length);
					break;
				case LIZ_CLTOMA_FUSE_SET_QUOTA:
					matoclserv_fuse_setquota(eptr, data, length);
					break;
				case LIZ_CLTOMA_FUSE_GET_QUOTA:
					matoclserv_fuse_getquota(eptr, data, length);
					break;
					/* do not use in version before 1.7.x */
				case CLTOMA_FUSE_GETXATTR:
					matoclserv_fuse_getxattr(eptr,data,length);
					break;
				case CLTOMA_FUSE_SETXATTR:
					matoclserv_fuse_setxattr(eptr,data,length);
					break;
					/* for tools - also should be available for registered clients */
				case CLTOMA_CSERV_LIST:
					matoclserv_cserv_list(eptr,data,length);
					break;
				case CLTOMA_SESSION_LIST:
					matoclserv_session_list(eptr,data,length);
					break;
				case CLTOAN_CHART:
					matoclserv_chart(eptr,data,length);
					break;
				case CLTOAN_CHART_DATA:
					matoclserv_chart_data(eptr,data,length);
					break;
				case CLTOMA_INFO:
					matoclserv_info(eptr,data,length);
					break;
				case CLTOMA_FSTEST_INFO:
					matoclserv_fstest_info(eptr,data,length);
					break;
				case CLTOMA_CHUNKSTEST_INFO:
					matoclserv_chunkstest_info(eptr,data,length);
					break;
				case CLTOMA_CHUNKS_MATRIX:
					matoclserv_chunks_matrix(eptr,data,length);
					break;
				case CLTOMA_EXPORTS_INFO:
					matoclserv_exports_info(eptr,data,length);
					break;
				case CLTOMA_MLOG_LIST:
					matoclserv_mlog_list(eptr,data,length);
					break;
				case CLTOMA_CSSERV_REMOVESERV:
					matoclserv_cserv_removeserv(eptr,data,length);
					break;
				case LIZ_CLTOMA_IOLIMIT:
					matoclserv_iolimit(eptr,data,length);
					break;
				case LIZ_CLTOMA_FUSE_SETLK:
					matoclserv_fuse_setlk(eptr,data,length);
					break;
				case LIZ_CLTOMA_FUSE_GETLK:
					matoclserv_fuse_getlk(eptr,data,length);
					break;
				case LIZ_CLTOMA_FUSE_FLOCK:
					matoclserv_fuse_flock(eptr,data,length);
					break;
				case LIZ_CLTOMA_FUSE_FLOCK_INTERRUPT:
					matoclserv_fuse_locks_interrupt(
						eptr, data, length,
						(uint8_t)lzfs_locks::Type::kFlock);
					break;
				case LIZ_CLTOMA_FUSE_SETLK_INTERRUPT:
					matoclserv_fuse_locks_interrupt(
						eptr, data, length,
						(uint8_t)lzfs_locks::Type::kPosix);
					break;
				case LIZ_CLTOMA_RECURSIVE_REMOVE:
					matoclserv_fuse_recursive_remove(eptr, data, length);
					break;
				case LIZ_CLTOMA_REQUEST_TASK_ID:
					matoclserv_fuse_request_task_id(eptr, data, length);
					break;
				case LIZ_CLTOMA_STOP_TASK:
					matoclserv_stop_task(eptr, data, length);
					break;
				case LIZ_CLTOMA_UPDATE_CREDENTIALS:
					matoclserv_update_credentials(eptr, data, length);
					break;
				case LIZ_CLTOMA_WHOLE_PATH_LOOKUP:
					matoclserv_liz_whole_path_lookup(eptr, data, length);
					break;
				case LIZ_CLTOMA_CSERV_LIST:
					matoclserv_liz_cserv_list(eptr, data, length);
					break;
				default:
					lzfs_pretty_syslog(LOG_NOTICE,"main master server module: got unknown message from mfsmount (type:%" PRIu32 ")",type);
					eptr->mode=KILL;
			}
		} else if (eptr->registered == ClientState::kOldTools) {        // old mfstools
			if (eptr->sesdata==NULL) {
				lzfs_pretty_syslog(LOG_ERR,"registered connection (tools) without sesdata !!!");
				eptr->mode=KILL;
				return;
			}
			switch (type) {
				// extra (external tools)
				case CLTOMA_FUSE_REGISTER:
					matoclserv_fuse_register(eptr,data,length);
					break;
				case CLTOMA_FUSE_READ_CHUNK: // used in mfsfileinfo
					matoclserv_fuse_read_chunk(eptr, PacketHeader(type, length), data);
					break;
				case CLTOMA_FUSE_CHECK:
					matoclserv_fuse_check(eptr,data,length);
					break;
				case CLTOMA_FUSE_GETTRASHTIME:
					matoclserv_fuse_gettrashtime(eptr,data,length);
					break;
				case CLTOMA_FUSE_SETTRASHTIME:
					matoclserv_fuse_settrashtime(eptr, PacketHeader(type, length), data);
					break;
				case CLTOMA_FUSE_GETGOAL:
					matoclserv_fuse_getgoal(eptr, PacketHeader(type, length), data);
					break;
				case CLTOMA_FUSE_SETGOAL:
					matoclserv_fuse_setgoal(eptr, PacketHeader(type, length), data);
					break;
				case CLTOMA_FUSE_APPEND:
					matoclserv_fuse_append(eptr,data,length);
					break;
				case CLTOMA_FUSE_GETDIRSTATS:
					matoclserv_fuse_getdirstats(eptr,data,length);
					break;
				case CLTOMA_FUSE_TRUNCATE:
					matoclserv_fuse_truncate(eptr, PacketHeader(type, length), data);
					break;
				case CLTOMA_FUSE_REPAIR:
					matoclserv_fuse_repair(eptr,data,length);
					break;
				case CLTOMA_FUSE_SNAPSHOT:
					matoclserv_fuse_snapshot(eptr, PacketHeader(type, length), data);
					break;
				case CLTOMA_FUSE_GETEATTR:
					matoclserv_fuse_geteattr(eptr,data,length);
					break;
				case CLTOMA_FUSE_SETEATTR:
					matoclserv_fuse_seteattr(eptr,data,length);
					break;
				default:
					lzfs_pretty_syslog(LOG_NOTICE,"main master server module: got unknown message from mfstools (type:%" PRIu32 ")",type);
					eptr->mode=KILL;
			}
		}
	} catch (IncorrectDeserializationException& e) {
		lzfs_pretty_syslog(LOG_NOTICE,
				"main master server module: got inconsistent message from mount "
				"(type:%" PRIu32 ", length:%" PRIu32"), %s", type, length, e.what());
		eptr->mode = KILL;
	}
}

void matoclserv_term(void) {
	matoclserventry *eptr,*eptrn;
	packetstruct *pptr,*pptrn;
	chunklist *cl,*cln;

	lzfs_pretty_syslog(LOG_NOTICE,"main master server module: closing %s:%s",ListenHost,ListenPort);
	tcpclose(lsock);

	for (eptr = matoclservhead ; eptr ; eptr = eptrn) {
		eptrn = eptr->next;
		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		for (pptr = eptr->outputhead ; pptr ; pptr = pptrn) {
			pptrn = pptr->next;
			if (pptr->packet) {
				free(pptr->packet);
			}
			free(pptr);
		}
		for (cl = eptr->chunkdelayedops ; cl ; cl = cln) {
			cln = cl->next;
			free(cl);
		}
		delete eptr;
	}
	matoclserv_session_unload();

	free(ListenHost);
	free(ListenPort);
}

void matoclserv_read(matoclserventry *eptr) {
	SignalLoopWatchdog watchdog;
	int32_t i;
	uint32_t type,size;
	const uint8_t *ptr;

	watchdog.start();
	while (eptr->mode != KILL) {
		i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
		if (i==0) {
			if (eptr->registered == ClientState::kRegistered) {       // show this message only for standard, registered clients
				lzfs_pretty_syslog(LOG_NOTICE,"connection with client(ip:%u.%u.%u.%u) has been closed by peer",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
			}
			eptr->mode = KILL;
			return;
		}
		if (i<0) {
			if (errno!=EAGAIN) {
#ifdef ECONNRESET
				if (errno!=ECONNRESET) {
#endif
					lzfs_silent_errlog(LOG_NOTICE,"main master server module: (ip:%u.%u.%u.%u) read error",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
#ifdef ECONNRESET
				}
#endif
				eptr->mode = KILL;
			}
			return;
		}
		eptr->inputpacket.startptr+=i;
		eptr->inputpacket.bytesleft-=i;
		stats_brcvd+=i;

		if (eptr->inputpacket.bytesleft>0) {
			return;
		}

		if (eptr->mode==HEADER) {
			ptr = eptr->hdrbuff+4;
			size = get32bit(&ptr);
			if (size>0) {
				if (size>MaxPacketSize) {
					lzfs_pretty_syslog(LOG_WARNING,"main master server module: packet too long (%" PRIu32 "/%u)",size,MaxPacketSize);
					eptr->mode = KILL;
					return;
				}
				eptr->inputpacket.packet = (uint8_t*) malloc(size);
				passert(eptr->inputpacket.packet);
				eptr->inputpacket.bytesleft = size;
				eptr->inputpacket.startptr = eptr->inputpacket.packet;
				eptr->mode = DATA;
				continue;
			}
			eptr->mode = DATA;
		}

		if (eptr->mode==DATA) {
			ptr = eptr->hdrbuff;
			type = get32bit(&ptr);
			size = get32bit(&ptr);

			eptr->mode=HEADER;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;
			matoclserv_gotpacket(eptr,type,eptr->inputpacket.packet,size);
			stats_prcvd++;

			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet=NULL;
			break;
		}

		if (watchdog.expired()) {
			break;
		}
	}
}

void matoclserv_write(matoclserventry *eptr) {
	SignalLoopWatchdog watchdog;
	packetstruct *pack;
	int32_t i;

	watchdog.start();
	for (;;) {
		pack = eptr->outputhead;
		if (pack==NULL) {
			return;
		}
		i=write(eptr->sock,pack->startptr,pack->bytesleft);
		if (i<0) {
			if (errno!=EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE,"main master server module: (ip:%u.%u.%u.%u) write error",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
				eptr->mode = KILL;
			}
			return;
		}
		pack->startptr+=i;
		pack->bytesleft-=i;
		stats_bsent+=i;
		if (pack->bytesleft>0) {
			return;
		}
		free(pack->packet);
		stats_psent++;
		eptr->outputhead = pack->next;
		if (eptr->outputhead==NULL) {
			eptr->outputtail = &(eptr->outputhead);
		}
		free(pack);

		if (watchdog.expired()) {
			break;
		}
	}
}

void matoclserv_wantexit(void) {
	exiting=1;
}

int matoclserv_canexit(void) {
	matoclserventry *adminTerminator = NULL;
	static bool terminatorPacketSent = false;
	for (matoclserventry* eptr = matoclservhead; eptr != nullptr; eptr = eptr->next) {
		if (eptr->outputhead!=NULL) {
			return 0;
		}
		if (eptr->chunkdelayedops!=NULL) {
			return 0;
		}
		if (eptr->adminTask == AdminTask::kTerminate) {
			adminTerminator = eptr;
		}
	}
	if (adminTerminator != NULL && !terminatorPacketSent) {
		// Are we replying to termination request?
		if (!matomlserv_canexit()){  // make sure there are no ml connected
			lzfs_pretty_syslog(LOG_INFO, "Waiting for ml connections to close...");
			return 0;
		} else {  // Reply to admin
			matoclserv_createpacket(adminTerminator,
					matocl::adminStopWithoutMetadataDump::build(LIZARDFS_STATUS_OK));
			terminatorPacketSent = true;
		}
	}
	// Wait for the admin which requested termination (if exists) to disconnect.
	// This ensures that he received the response (or died and is no longer interested).
	for (matoclserventry* eptr = matoclservhead; eptr != nullptr; eptr = eptr->next) {
		if (eptr->adminTask == AdminTask::kTerminate) {
			return 0;
		}
	}
	return 1;
}

void matoclserv_desc(std::vector<pollfd> &pdesc) {
	matoclserventry *eptr;

	if (exiting==0) {
		pdesc.push_back({lsock,POLLIN,0});
		lsockpdescpos = pdesc.size() - 1;
	} else {
		lsockpdescpos = -1;
	}
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		pdesc.push_back({eptr->sock,0,0});
		eptr->pdescpos = pdesc.size() - 1;
		if (exiting==0) {
			pdesc.back().events |= POLLIN;
		}
		if (eptr->outputhead!=NULL) {
			pdesc.back().events |= POLLOUT;
		}
	}
}


void matoclserv_serve(const std::vector<pollfd> &pdesc) {
	uint32_t now=eventloop_time();
	matoclserventry *eptr,**kptr;
	packetstruct *pptr,*paptr;
	int ns;

	if (lsockpdescpos>=0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
		ns=tcpaccept(lsock);
		if (ns<0) {
			lzfs_silent_errlog(LOG_NOTICE,"main master server module: accept error");
		} else {
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = new matoclserventry;
			eptr->next = matoclservhead;
			matoclservhead = eptr;
			eptr->sock = ns;
			eptr->pdescpos = -1;
			tcpgetpeer(ns,&(eptr->peerip),NULL);
			eptr->registered = ClientState::kUnregistered;
			eptr->iolimits = false;
			eptr->version = 0;
			eptr->mode = HEADER;
			eptr->lastread = now;
			eptr->lastwrite = now;
			eptr->inputpacket.next = NULL;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;
			eptr->inputpacket.packet = NULL;
			eptr->adminTask = AdminTask::kNone;
			eptr->outputhead = NULL;
			eptr->outputtail = &(eptr->outputhead);

			eptr->chunkdelayedops = NULL;
			eptr->sesdata = NULL;
			memset(eptr->passwordrnd,0,32);
		}
	}

// read
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP)) {
				eptr->mode = KILL;
			}
			if ((pdesc[eptr->pdescpos].revents & POLLIN) && eptr->mode!=KILL) {
				eptr->lastread = now;
				matoclserv_read(eptr);
			}
		}
	}

// write
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		if (eptr->lastwrite+2<now && eptr->registered != ClientState::kOldTools
				&& eptr->outputhead==NULL) {
			uint8_t *ptr = matoclserv_createpacket(eptr,ANTOAN_NOP,4);      // 4 byte length because of 'msgid'
			*((uint32_t*)ptr) = 0;
		}
		if (eptr->pdescpos>=0) {
			if ((((pdesc[eptr->pdescpos].events & POLLOUT)==0 && (eptr->outputhead)) || (pdesc[eptr->pdescpos].revents & POLLOUT)) && eptr->mode!=KILL) {
				eptr->lastwrite = now;
				matoclserv_write(eptr);
			}
		}
		if (eptr->lastread+10<now && exiting==0) {
			eptr->mode = KILL;
		}
	}

// close
	kptr = &matoclservhead;
	while ((eptr=*kptr)) {
		if (eptr->mode == KILL) {
			matocl_beforedisconnect(eptr);
			tcpclose(eptr->sock);
			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			pptr = eptr->outputhead;
			while (pptr) {
				if (pptr->packet) {
					free(pptr->packet);
				}
				paptr = pptr;
				pptr = pptr->next;
				free(paptr);
			}
			*kptr = eptr->next;
			delete eptr;
		} else {
			kptr = &(eptr->next);
		}
	}
}

void matoclserv_start_cond_check(void) {
	if (starting) {
// very simple condition checking if all chunkservers have been connected
// in the future master will know his chunkservers list and then this condition will be changed
		if (chunk_get_missing_count()<100) {
			starting=0;
		} else {
			starting--;
		}
	}
}

int matoclserv_sessionsinit(void) {
	sessionshead = NULL;

	switch (matoclserv_load_sessions()) {
		case 0: // no file
			lzfs_pretty_syslog(LOG_WARNING,"sessions file %s/%s not found;"
					" if it is not a fresh installation you have to restart all active mounts",
					fs::getCurrentWorkingDirectoryNoThrow().c_str(), kSessionsFilename);
			matoclserv_store_sessions();
			break;
		case 1: // file loaded
			lzfs_pretty_syslog(LOG_INFO,"initialized sessions from file %s/%s",
					fs::getCurrentWorkingDirectoryNoThrow().c_str(), kSessionsFilename);
			break;
		default:
			lzfs_pretty_syslog(LOG_ERR,"due to missing sessions (%s/%s)"
					" you have to restart all active mounts",
					fs::getCurrentWorkingDirectoryNoThrow().c_str(), kSessionsFilename);
			break;
	}
	SessionSustainTime = cfg_getuint32("SESSION_SUSTAIN_TIME",86400);
	if (SessionSustainTime>7*86400) {
		SessionSustainTime=7*86400;
		lzfs_pretty_syslog(LOG_WARNING,"SESSION_SUSTAIN_TIME too big (more than week) - setting this value to one week");
	}
	if (SessionSustainTime<60) {
		SessionSustainTime=60;
		lzfs_pretty_syslog(LOG_WARNING,"SESSION_SUSTAIN_TIME too low (less than minute) - setting this value to one minute");
	}
	return 0;
}

int matoclserv_iolimits_reload() {
	std::string configFile = cfg_getstring("GLOBALIOLIMITS_FILENAME", "");
	gIoLimitsAccumulate_ms = cfg_get_minvalue("GLOBALIOLIMITS_ACCUMULATE_MS", 250U, 1U);

	if (!configFile.empty()) {
		try {
			IoLimitsConfigLoader configLoader;
			configLoader.load(std::ifstream(configFile));
			gIoLimitsSubsystem = configLoader.subsystem();
			gIoLimitsDatabase.setLimits(
					SteadyClock::now(), configLoader.limits(), gIoLimitsAccumulate_ms);
		} catch (Exception& ex) {
			lzfs_pretty_syslog(LOG_ERR, "failed to process global I/O limits configuration "
					"file (%s): %s", configFile.c_str(), ex.message().c_str());
			return -1;
		}
	} else {
		gIoLimitsSubsystem = "";
		gIoLimitsDatabase.setLimits(
				SteadyClock::now(), IoLimitsConfigLoader::LimitsMap(), gIoLimitsAccumulate_ms);
	}

	gIoLimitsRefreshTime = cfg_get_minvalue(
			"GLOBALIOLIMITS_RENEGOTIATION_PERIOD_SECONDS", 0.1, 0.001);

	gIoLimitsConfigId++;

	matoclserv_broadcast_iolimits_cfg();

	return 0;
}

void  matoclserv_become_master() {
	starting = 120;
	matoclserv_reset_session_timeouts();
	matoclserv_start_cond_check();
	if (starting) {
		eventloop_timeregister(TIMEMODE_RUN_LATE,1,0,matoclserv_start_cond_check);
	}
	eventloop_timeregister(TIMEMODE_RUN_LATE,10,0,matocl_session_check);
	eventloop_timeregister(TIMEMODE_RUN_LATE,3600,0,matocl_session_statsmove);
	return;
}

void matoclserv_reload(void) {
	// Notify admins that reload was performed - put responses in their packet queues
	for (matoclserventry* eptr = matoclservhead; eptr != nullptr; eptr = eptr->next) {
		if (eptr->adminTask == AdminTask::kReload) {
			matoclserv_createpacket(eptr, matocl::adminReload::build(LIZARDFS_STATUS_OK));
			eptr->adminTask = AdminTask::kNone;
		}
	}

	RejectOld = cfg_getuint32("REJECT_OLD_CLIENTS",0);
	SessionSustainTime = cfg_getuint32("SESSION_SUSTAIN_TIME",86400);
	if (SessionSustainTime>7*86400) {
		SessionSustainTime=7*86400;
		lzfs_pretty_syslog(LOG_WARNING,"SESSION_SUSTAIN_TIME too big (more than week) - setting this value to one week");
	}
	if (SessionSustainTime<60) {
		SessionSustainTime=60;
		lzfs_pretty_syslog(LOG_WARNING,"SESSION_SUSTAIN_TIME too low (less than minute) - setting this value to one minute");
	}

	matoclserv_iolimits_reload();

	char *oldListenHost = ListenHost;
	char *oldListenPort = ListenPort;
	if (cfg_isdefined("MATOCL_LISTEN_HOST") || cfg_isdefined("MATOCL_LISTEN_PORT") || !(cfg_isdefined("MATOCU_LISTEN_HOST") || cfg_isdefined("MATOCU_LISTEN_HOST"))) {
		ListenHost = cfg_getstr("MATOCL_LISTEN_HOST","*");
		ListenPort = cfg_getstr("MATOCL_LISTEN_PORT","9421");
	} else {
		ListenHost = cfg_getstr("MATOCU_LISTEN_HOST","*");
		ListenPort = cfg_getstr("MATOCU_LISTEN_PORT","9421");
	}
	if (strcmp(oldListenHost,ListenHost)==0 && strcmp(oldListenPort,ListenPort)==0) {
		free(oldListenHost);
		free(oldListenPort);
		lzfs_pretty_syslog(LOG_NOTICE,"main master server module: socket address hasn't changed (%s:%s)",ListenHost,ListenPort);
		return;
	}

	int newlsock = tcpsocket();
	if (newlsock<0) {
		lzfs_pretty_errlog(LOG_WARNING,"main master server module: socket address has changed, but can't create new socket");
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
		lzfs_silent_errlog(LOG_NOTICE,"main master server module: can't set accept filter");
	}
	if (tcpstrlisten(newlsock,ListenHost,ListenPort,100)<0) {
		lzfs_pretty_errlog(LOG_ERR,"main master server module: socket address has changed, but can't listen on socket (%s:%s)",ListenHost,ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		tcpclose(newlsock);
		return;
	}
	lzfs_pretty_syslog(LOG_NOTICE,"main master server module: socket address has changed, now listen on %s:%s",ListenHost,ListenPort);
	free(oldListenHost);
	free(oldListenPort);
	tcpclose(lsock);
	lsock = newlsock;
}

int matoclserv_networkinit(void) {
	if (cfg_isdefined("MATOCL_LISTEN_HOST") || cfg_isdefined("MATOCL_LISTEN_PORT") || !(cfg_isdefined("MATOCU_LISTEN_HOST") || cfg_isdefined("MATOCU_LISTEN_HOST"))) {
		ListenHost = cfg_getstr("MATOCL_LISTEN_HOST","*");
		ListenPort = cfg_getstr("MATOCL_LISTEN_PORT","9421");
	} else {
		lzfs_pretty_syslog(LOG_WARNING, "options MATOCU_LISTEN_* are deprecated -- use "
				"MATOCL_LISTEN_* instead");
		ListenHost = cfg_getstr("MATOCU_LISTEN_HOST","*");
		ListenPort = cfg_getstr("MATOCU_LISTEN_PORT","9421");
	}
	RejectOld = cfg_getuint32("REJECT_OLD_CLIENTS",0);

	if (matoclserv_iolimits_reload() != 0) {
		return -1;
	}

	exiting = 0;
	lsock = tcpsocket();
	if (lsock<0) {
		lzfs_pretty_errlog(LOG_ERR,"main master server module: can't create socket");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		lzfs_silent_errlog(LOG_NOTICE,"main master server module: can't set accept filter");
	}
	if (tcpstrlisten(lsock,ListenHost,ListenPort,100)<0) {
		lzfs_pretty_errlog(LOG_ERR,"main master server module: can't listen on %s:%s",ListenHost,ListenPort);
		return -1;
	}
	lzfs_pretty_syslog(LOG_NOTICE,"main master server module: listen on %s:%s",ListenHost,ListenPort);

	matoclservhead = NULL;

	if (metadataserver::isMaster()) {
		matoclserv_become_master();
	}
	eventloop_reloadregister(matoclserv_reload);
	metadataserver::registerFunctionCalledOnPromotion(matoclserv_become_master);
	eventloop_destructregister(matoclserv_term);
	eventloop_pollregister(matoclserv_desc,matoclserv_serve);
	eventloop_wantexitregister(matoclserv_wantexit);
	eventloop_canexitregister(matoclserv_canexit);
	return 0;
}

void matoclserv_session_unload(void) {
	for (session* ss = sessionshead, *ssn = NULL; ss ; ss = ssn) {
		ssn = ss->next;
		for (filelist* of = ss->openedfiles, *ofn = NULL; of; of = ofn) {
			ofn = of->next;
			free(of);
		}
		if (ss->info) {
			free(ss->info);
		}
		delete ss;
	}
	sessionshead = nullptr;
}
