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
#include "common/chunk_with_address_and_label.h"
#include "common/cltoma_communication.h"
#include "common/datapack.h"
#include "common/io_limits_config_loader.h"
#include "common/io_limits_database.h"
#include "common/lizardfs_version.h"
#include "common/main.h"
#include "common/massert.h"
#include "common/matocl_communication.h"
#include "common/md5.h"
#include "common/metadata.h"
#include "common/MFSCommunication.h"
#include "common/random.h"
#include "common/serialized_goal.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "master/changelog.h"
#include "master/chartsdata.h"
#include "master/chunks.h"
#include "master/datacachemgr.h"
#include "master/exports.h"
#include "master/filesystem.h"
#include "master/masterconn.h"
#include "master/matocsserv.h"
#include "master/matomlserv.h"
#include "master/personality.h"

#define MaxPacketSize 1000000

// matoclserventry.mode
enum {KILL,HEADER,DATA};
// chunklis.type
enum {FUSE_WRITE,FUSE_TRUNCATE};

#define SESSION_STATS 16

const uint32_t kMaxNumberOfChunkCopies = 100U;

/* CACHENOTIFY
// hash size should be at least 1.5 * 10000 * # of connected mounts
// it also should be the prime number
// const 10000 is defined in mfsmount/dircache.c file as DIRS_REMOVE_THRESHOLD_MAX
// current const is calculated as nextprime(1.5 * 10000 * 500) and is enough for up to about 500 mounts
#define DIRINODE_HASH_SIZE 7500013
*/

struct matoclserventry;

/* CACHENOTIFY
// directories in external caches
typedef struct dirincache {
	struct matoclserventry *eptr;
	uint32_t dirinode;
	struct dirincache *nextnode,**prevnode;
	struct dirincache *nextcu,**prevcu;
} dirincache;

static dirincache **dirinodehash;
*/

// locked chunks
typedef struct chunklist {
	uint64_t chunkid;
	uint64_t fleng;         // file length
	uint32_t qid;           // queryid for answer
	uint32_t inode;         // inode
	uint32_t uid;
	uint32_t gid;
	uint32_t auid;
	uint32_t agid;
	uint8_t type;
	struct chunklist *next;
} chunklist;

// opened files
typedef struct filelist {
	uint32_t inode;
	struct filelist *next;
} filelist;

typedef struct session {
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
	uint32_t currentopstats[SESSION_STATS];
	uint32_t lasthouropstats[SESSION_STATS];
	filelist *openedfiles;
	struct session *next;
} session;

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

/** Client entry in the server. */
struct matoclserventry {
	ClientState registered;
	uint8_t mode;                           //0 - not active, 1 - read header, 2 - read packet
	bool iolimits;
/* CACHENOTIFY
	uint8_t notifications;
*/
	int sock;                               //socket number
	int32_t pdescpos;
	uint32_t lastread,lastwrite;            //time of last activity
	uint32_t version;
	uint32_t peerip;
	uint8_t hdrbuff[8];
	packetstruct inputpacket;
	packetstruct *outputhead,**outputtail;
	bool requestingTermination;       // Is this client requesting termination of the master server

	uint8_t passwordrnd[32];
	session *sesdata;
	std::unique_ptr<LizMatoclAdminRegisterChallengeData> adminChallenge;
	chunklist *chunkdelayedops;
/* CACHENOTIFY
	dirincache *cacheddirs;
*/

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

/* CACHENOTIFY
// cache notification routines

static inline void matoclserv_dircache_init(void) {
	dirinodehash = (dirincache**)malloc(sizeof(dirincache*)*DIRINODE_HASH_SIZE);
	passert(dirinodehash);
}

static inline void matoclserv_dircache_remove_entry(dirincache *dc) {
	*(dc->prevnode) = dc->nextnode;
	if (dc->nextnode) {
		dc->nextnode->prevnode = dc->prevnode;
	}
	*(dc->prevcu) = dc->nextcu;
	if (dc->nextcu) {
		dc->nextcu->prevcu = dc->prevcu;
	}
	free(dc);
}

static inline void matoclserv_notify_add_dir(matoclserventry *eptr,uint32_t inode) {
	uint32_t hash = (inode*0x5F2318BD)%DIRINODE_HASH_SIZE;
	dirincache *dc;

	dc = (dirincache*)malloc(sizeof(dirincache));
	passert(dc);
	dc->eptr = eptr;
	dc->dirinode = inode;
	// by inode
	dc->nextnode = dirinodehash[hash];
	dc->prevnode = (dirinodehash+hash);
	if (dirinodehash[hash]) {
		dirinodehash[hash]->prevnode = &(dc->nextnode);
	}
	dirinodehash[hash] = dc;
	// by eptr
	dc->nextcu = eptr->cacheddirs;
	dc->prevcu = &(eptr->cacheddirs);
	if (eptr->cacheddirs) {
		eptr->cacheddirs->prevcu = &(dc->nextcu);
	}
	eptr->cacheddirs = dc;

//      syslog(LOG_NOTICE,"rcvd from: '%s' ; add inode: %" PRIu32,eptr->sesdata->info,inode);
}

static inline void matoclserv_notify_remove_dir(matoclserventry *eptr,uint32_t inode) {
	uint32_t hash = (inode*0x5F2318BD)%DIRINODE_HASH_SIZE;
	dirincache *dc,*ndc;

	for (dc=dirinodehash[hash] ; dc ; dc=ndc) {
		ndc = dc->nextnode;
		if (dc->eptr==eptr && dc->dirinode==inode) {
			matoclserv_dircache_remove_entry(dc);
		}
	}
//      syslog(LOG_NOTICE,"rcvd from: '%s' ; remove inode: %" PRIu32,eptr->sesdata->info,inode);
}

static inline void matoclserv_notify_disconnected(matoclserventry *eptr) {
	while (eptr->cacheddirs) {
		matoclserv_dircache_remove_entry(eptr->cacheddirs);
	}
}

static inline void matoclserv_show_notification_dirs(void) {
	uint32_t hash;
	dirincache *dc;

	for (hash=0 ; hash<DIRINODE_HASH_SIZE ; hash++) {
		for (dc=dirinodehash[hash] ; dc ; dc=dc->nextnode) {
			syslog(LOG_NOTICE,"session: %u ; dir inode: %u",dc->eptr->sesdata->sessionid,dc->dirinode);
		}
	}
}
*/

/* new registration procedure */
session* matoclserv_new_session(uint8_t newsession,uint8_t nonewid) {
	session *asesdata;
	asesdata = (session*)malloc(sizeof(session));
	passert(asesdata);
	if (newsession==0 && nonewid) {
		asesdata->sessionid = 0;
	} else {
		asesdata->sessionid = fs_newsessionid();
	}
	asesdata->info = NULL;
	asesdata->peerip = 0;
	asesdata->sesflags = 0;
	asesdata->rootuid = 0;
	asesdata->rootgid = 0;
	asesdata->mapalluid = 0;
	asesdata->mapallgid = 0;
	asesdata->newsession = newsession;
	asesdata->rootinode = MFS_ROOT_ID;
	asesdata->openedfiles = NULL;
	asesdata->disconnected = 0;
	asesdata->nsocks = 1;
	memset(asesdata->currentopstats,0,4*SESSION_STATS);
	memset(asesdata->lasthouropstats,0,4*SESSION_STATS);
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
		syslog(LOG_WARNING,"can't store sessions, fwrite error");
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
				syslog(LOG_WARNING,"can't store sessions, fwrite error");
				fclose(fd);
				return;
			}
			if (ileng>0) {
				if (fwrite(asesdata->info,ileng,1,fd)!=1) {
					syslog(LOG_WARNING,"can't store sessions, fwrite error");
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
		syslog(LOG_WARNING,"can't load sessions, fread error");
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
			syslog(LOG_WARNING,"can't load sessions, fread error");
			fclose(fd);
			return -1;
		}
		ptr = hdr;
		statsinfile = get16bit(&ptr);
	} else if (memcmp(hdr,MFSSIGNATURE "S \001\006\004",8)==0) {
		mapalldata = 1;
		goaltrashdata = 1;
		if (fread(hdr,2,1,fd)!=1) {
			syslog(LOG_WARNING,"can't load sessions, fread error");
			fclose(fd);
			return -1;
		}
		ptr = hdr;
		statsinfile = get16bit(&ptr);
	} else {
		syslog(LOG_WARNING,"can't load sessions, bad header");
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
			asesdata = (session*)malloc(sizeof(session));
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
			} else { // set defaults (no limits)
				asesdata->mingoal = goal::kMinGoal;
				asesdata->maxgoal = goal::kMaxGoal;
				asesdata->mintrashtime = 0;
				asesdata->maxtrashtime = UINT32_C(0xFFFFFFFF);
			}
			asesdata->rootuid = get32bit(&ptr);
			asesdata->rootgid = get32bit(&ptr);
			if (mapalldata) {
				asesdata->mapalluid = get32bit(&ptr);
				asesdata->mapallgid = get32bit(&ptr);
			} else {
				asesdata->mapalluid = 0;
				asesdata->mapallgid = 0;
			}
			asesdata->info = NULL;
			asesdata->newsession = 1;
			asesdata->openedfiles = NULL;
			asesdata->disconnected = main_time();
			asesdata->nsocks = 0;
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
					free(asesdata);
					free(fsesrecord);
					syslog(LOG_WARNING,"can't load sessions, fread error");
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
			syslog(LOG_WARNING,"can't load sessions, fread error");
			fclose(fd);
			return -1;
		}
	}
	free(fsesrecord);
	syslog(LOG_NOTICE,"sessions have been loaded");
	fclose(fd);
	return 1;
}

int matoclserv_insert_openfile(session* cr,uint32_t inode) {
	filelist *ofptr,**ofpptr;
	int status;

	ofpptr = &(cr->openedfiles);
	while ((ofptr=*ofpptr)) {
		if (ofptr->inode==inode) {
			return STATUS_OK;       // file already acquired - nothing to do
		}
		if (ofptr->inode>inode) {
			break;
		}
		ofpptr = &(ofptr->next);
	}
	status = fs_acquire(FsContext::getForMaster(main_time()), inode, cr->sessionid);
	if (status==STATUS_OK) {
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
		asesdata = (session*)malloc(sizeof(session));
		passert(asesdata);
		asesdata->sessionid = sessionid;
/* session created by filesystem - only for old clients (pre 1.5.13) */
		asesdata->info = NULL;
		asesdata->peerip = 0;
		asesdata->sesflags = 0;
		asesdata->mingoal = goal::kMinGoal;
		asesdata->maxgoal = goal::kMaxGoal;
		asesdata->mintrashtime = 0;
		asesdata->maxtrashtime = UINT32_C(0xFFFFFFFF);
		asesdata->rootuid = 0;
		asesdata->rootgid = 0;
		asesdata->mapalluid = 0;
		asesdata->mapallgid = 0;
		asesdata->newsession = 0;
		asesdata->rootinode = MFS_ROOT_ID;
		asesdata->openedfiles = NULL;
		asesdata->disconnected = main_time();
		asesdata->nsocks = 0;
		memset(asesdata->currentopstats,0,4*SESSION_STATS);
		memset(asesdata->lasthouropstats,0,4*SESSION_STATS);
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
		syslog(LOG_ERR, "sessions file is corrupted");
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
	uint32_t now = main_time();
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

void matoclserv_chunk_status(uint64_t chunkid,uint8_t status) {
	uint32_t qid,inode,uid,gid,auid,agid;
	uint64_t fleng;
	uint8_t type,attr[35];
	uint32_t version;
	uint8_t *ptr;
	uint8_t count;
	uint8_t loc[100*6];
	chunklist *cl,**acl;
	matoclserventry *eptr,*eaptr;

	eptr=NULL;
	qid=0;
	fleng=0;
	type=0;
	inode=0;
	uid=0;
	gid=0;
	auid=0;
	agid=0;
	for (eaptr = matoclservhead ; eaptr && eptr==NULL ; eaptr=eaptr->next) {
		if (eaptr->mode!=KILL) {
			acl = &(eaptr->chunkdelayedops);
			while (*acl && eptr==NULL) {
				cl = *acl;
				if (cl->chunkid==chunkid) {
					eptr = eaptr;
					qid = cl->qid;
					fleng = cl->fleng;
					type = cl->type;
					inode = cl->inode;
					uid = cl->uid;
					gid = cl->gid;
					auid = cl->auid;
					agid = cl->agid;

					*acl = cl->next;
					free(cl);
				} else {
					acl = &(cl->next);
				}
			}
		}
	}

	if (!eptr) {
		syslog(LOG_WARNING,"got chunk status, but don't want it");
		return;
	}
	if (status==STATUS_OK) {
		dcm_modify(inode,eptr->sesdata->sessionid);
	}
	switch (type) {
	case FUSE_WRITE:
		if (status==STATUS_OK) {
			status=chunk_getversionandlocations(chunkid,eptr->peerip,&version,&count,loc);
			//syslog(LOG_NOTICE,"get version for chunk %" PRIu64 " -> %" PRIu32,chunkid,version);
		}
		if (status!=STATUS_OK) {
			ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_WRITE_CHUNK,5);
			put32bit(&ptr,qid);
			put8bit(&ptr,status);
			fs_writeend(0,0,chunkid);       // ignore status - just do it.
			return;
		}
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_WRITE_CHUNK,24+count*6);
		put32bit(&ptr,qid);
		put64bit(&ptr,fleng);
		put64bit(&ptr,chunkid);
		put32bit(&ptr,version);
		memcpy(ptr,loc,count*6);
		return;
	case FUSE_TRUNCATE:
		fs_end_setlength(chunkid);

		if (status!=STATUS_OK) {
			ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_TRUNCATE,5);
			put32bit(&ptr,qid);
			put8bit(&ptr,status);
			return;
		}
		fs_do_setlength(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,uid,gid,auid,agid,fleng,attr);
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_TRUNCATE,39);
		put32bit(&ptr,qid);
		memcpy(ptr,attr,35);
		return;
	default:
		syslog(LOG_WARNING,"got chunk status, but operation type is unknown");
	}
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

/**
 * Returns FsContext with session data but without uid/gid data
 */
static inline FsContext matoclserv_get_context(matoclserventry *eptr) {
	return FsContext::getForMaster(main_time(), eptr->sesdata->rootinode, eptr->sesdata->sesflags);
}

/**
 * Returns FsContext with session data and uid/gid data
 */
static inline FsContext matoclserv_get_context(matoclserventry *eptr, uint32_t uid, uint32_t gid) {
	uint32_t auid = uid;
	uint32_t agid = gid;
	matoclserv_ugid_remap(eptr, &uid, &gid);
	return FsContext::getForMasterWithSession(main_time(),
			eptr->sesdata->rootinode, eptr->sesdata->sesflags, uid, gid, auid, agid);
}

void matoclserv_cserv_list(matoclserventry *eptr, const uint8_t */*data*/, uint32_t length) {
	if (length!=0) {
		syslog(LOG_NOTICE,"CLTOMA_CSERV_LIST - wrong size (%" PRIu32 "/0)",length);
		eptr->mode = KILL;
		return;
	}
	auto listOfChunkservers = matocsserv_cservlist();
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

void matoclserv_liz_cserv_list(matoclserventry *eptr) {
	MessageBuffer buffer;
	matocl::cservList::serialize(buffer, matocsserv_cservlist());
	matoclserv_createpacket(eptr, std::move(buffer));
}

void matoclserv_cserv_removeserv(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t ip;
	uint16_t port;
	if (length!=6) {
		syslog(LOG_NOTICE,"CLTOMA_CSSERV_REMOVESERV - wrong size (%" PRIu32 "/6)",length);
		eptr->mode = KILL;
		return;
	}
	ip = get32bit(&data);
	port = get16bit(&data);
	matocsserv_csdb_remove_server(ip,port);
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
	std::vector<SerializedGoal> serializedGoals;
	const GoalMap<Goal>& goalMap = fs_get_goal_definitions();
	for (unsigned i = goal::kMinGoal; i <= goal::kMaxGoal; ++i) {
		const Goal& goal = goalMap[i];
		std::stringstream ss;
		bool first = true;
		for (const Goal::Labels::value_type& labelCount : goal.labels()) {
			if (first) {
				first = false;
			} else {
				ss << ',';
			}
			ss << labelCount.second << "*" << labelCount.first;
		}
		serializedGoals.emplace_back(i, goal.name(), ss.str());
	}
	matoclserv_createpacket(eptr, matocl::listGoals::build(serializedGoals));
}

void matoclserv_chunks_health(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	bool regularChunksOnly;
	cltoma::chunksHealth::deserialize(data, length, regularChunksOnly);
	auto message = matocl::chunksHealth::build(regularChunksOnly,
			chunk_get_availability_state(regularChunksOnly),
			chunk_get_replication_state(regularChunksOnly));
	matoclserv_createpacket(eptr, std::move(message));
}

void matoclserv_session_list(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	matoclserventry *eaptr;
	uint32_t size,ileng,pleng,i;
	uint8_t vmode;
	(void)data;
	if (length!=0 && length!=1) {
		syslog(LOG_NOTICE,"CLTOMA_SESSION_LIST - wrong size (%" PRIu32 "/0)",length);
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
		syslog(LOG_NOTICE,"CLTOAN_CHART - wrong size (%" PRIu32 "/4)",length);
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
		syslog(LOG_NOTICE,"CLTOAN_CHART_DATA - wrong size (%" PRIu32 "/4)",length);
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
	uint64_t totalspace,availspace,trspace,respace;
	uint64_t memusage;
	uint32_t trnodes,renodes,inodes,dnodes,fnodes;
	uint32_t chunks,chunkcopies,tdcopies;
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CLTOMA_INFO - wrong size (%" PRIu32 "/0)",length);
		eptr->mode = KILL;
		return;
	}
	fs_info(&totalspace,&availspace,&trspace,&trnodes,&respace,&renodes,&inodes,&dnodes,&fnodes);
	chunk_info(&chunks,&chunkcopies,&tdcopies);
	memusage = chartsdata_memusage();
	ptr = matoclserv_createpacket(eptr,MATOCL_INFO,76);
	put16bit(&ptr,LIZARDFS_PACKAGE_VERSION_MAJOR);
	put8bit(&ptr,LIZARDFS_PACKAGE_VERSION_MINOR);
	put8bit(&ptr,LIZARDFS_PACKAGE_VERSION_MICRO);
	/* --- */
	put64bit(&ptr,memusage);
	/* --- */
	put64bit(&ptr,totalspace);
	put64bit(&ptr,availspace);
	put64bit(&ptr,trspace);
	put32bit(&ptr,trnodes);
	put64bit(&ptr,respace);
	put32bit(&ptr,renodes);
	put32bit(&ptr,inodes);
	put32bit(&ptr,dnodes);
	put32bit(&ptr,fnodes);
	put32bit(&ptr,chunks);
	put32bit(&ptr,chunkcopies);
	put32bit(&ptr,tdcopies);
}

void matoclserv_fstest_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t loopstart,loopend,files,ugfiles,mfiles,chunks,ugchunks,mchunks,msgbuffleng;
	char *msgbuff;
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CLTOMA_FSTEST_INFO - wrong size (%" PRIu32 "/0)",length);
		eptr->mode = KILL;
		return;
	}
	fs_test_getdata(&loopstart,&loopend,&files,&ugfiles,&mfiles,&chunks,&ugchunks,&mchunks,&msgbuff,&msgbuffleng);
	ptr = matoclserv_createpacket(eptr,MATOCL_FSTEST_INFO,msgbuffleng+36);
	put32bit(&ptr,loopstart);
	put32bit(&ptr,loopend);
	put32bit(&ptr,files);
	put32bit(&ptr,ugfiles);
	put32bit(&ptr,mfiles);
	put32bit(&ptr,chunks);
	put32bit(&ptr,ugchunks);
	put32bit(&ptr,mchunks);
	put32bit(&ptr,msgbuffleng);
	if (msgbuffleng>0) {
		memcpy(ptr,msgbuff,msgbuffleng);
	}
}

void matoclserv_chunkstest_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CLTOMA_CHUNKSTEST_INFO - wrong size (%" PRIu32 "/0)",length);
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
		syslog(LOG_NOTICE,"CLTOMA_CHUNKS_MATRIX - wrong size (%" PRIu32 "/0|1)",length);
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
		syslog(LOG_NOTICE,"CLTOMA_EXPORTS_INFO - wrong size (%" PRIu32 "/0|1)",length);
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
		syslog(LOG_NOTICE,"CLTOMA_MLOG_LIST - wrong size (%" PRIu32 "/0)",length);
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

/* CACHENOTIFY
void matoclserv_notify_attr(uint32_t dirinode,uint32_t inode,const Attributes& attr) {
	uint32_t hash = (dirinode*0x5F2318BD)%DIRINODE_HASH_SIZE;
	dirincache *dc;
	uint8_t *ptr;

	for (dc=dirinodehash[hash] ; dc ; dc=dc->nextnode) {
		if (dc->dirinode==dirinode) {
//                      syslog(LOG_NOTICE,"send to: '%s' ; attrs of inode: %" PRIu32,dc->eptr->sesdata->info,inode);
			ptr = matoclserv_createpacket(dc->eptr,MATOCL_FUSE_NOTIFY_ATTR,43);
			stats_notify++;
			put32bit(&ptr,0);
			put32bit(&ptr,inode);
			memcpy(ptr,attr,35);
			if (dc->eptr->sesdata) {
				dc->eptr->sesdata->currentopstats[16]++;
			}
			dc->eptr->notifications = 1;
		}
	}
}

void matoclserv_notify_link(uint32_t dirinode,uint8_t nleng,const uint8_t *name,uint32_t inode,const Attributes& attr,uint32_t ts) {
	uint32_t hash = (dirinode*0x5F2318BD)%DIRINODE_HASH_SIZE;
	dirincache *dc;
	uint8_t *ptr;

	for (dc=dirinodehash[hash] ; dc ; dc=dc->nextnode) {
		if (dc->dirinode==dirinode) {
//                      {
//                              char strname[256];
//                              memcpy(strname,name,nleng);
//                              strname[nleng]=0;
//                              syslog(LOG_NOTICE,"send to: '%s' ; new link (%" PRIu32 ",%s)->%" PRIu32,dc->eptr->sesdata->info,dirinode,strname,inode);
//                      }
			ptr = matoclserv_createpacket(dc->eptr,MATOCL_FUSE_NOTIFY_LINK,52+nleng);
			stats_notify++;
			put32bit(&ptr,0);
			put32bit(&ptr,ts);
			if (dirinode==dc->eptr->sesdata->rootinode) {
				put32bit(&ptr,MFS_ROOT_ID);
			} else {
				put32bit(&ptr,dirinode);
			}
			put8bit(&ptr,nleng);
			memcpy(ptr,name,nleng);
			ptr+=nleng;
			put32bit(&ptr,inode);
			memcpy(ptr,attr,35);
			if (dc->eptr->sesdata) {
				dc->eptr->sesdata->currentopstats[17]++;
			}
			dc->eptr->notifications = 1;
		}
	}
}

void matoclserv_notify_unlink(uint32_t dirinode,uint8_t nleng,const uint8_t *name,uint32_t ts) {
	uint32_t hash = (dirinode*0x5F2318BD)%DIRINODE_HASH_SIZE;
	dirincache *dc;
	uint8_t *ptr;

	for (dc=dirinodehash[hash] ; dc ; dc=dc->nextnode) {
		if (dc->dirinode==dirinode) {
//                      {
//                              char strname[256];
//                              memcpy(strname,name,nleng);
//                              strname[nleng]=0;
//                              syslog(LOG_NOTICE,"send to: '%s' ; remove link (%" PRIu32 ",%s)",dc->eptr->sesdata->info,dirinode,strname);
//                      }
			ptr = matoclserv_createpacket(dc->eptr,MATOCL_FUSE_NOTIFY_UNLINK,13+nleng);
			stats_notify++;
			put32bit(&ptr,0);
			put32bit(&ptr,ts);
			if (dirinode==dc->eptr->sesdata->rootinode) {
				put32bit(&ptr,MFS_ROOT_ID);
			} else {
				put32bit(&ptr,dirinode);
			}
			put8bit(&ptr,nleng);
			memcpy(ptr,name,nleng);
			if (dc->eptr->sesdata) {
				dc->eptr->sesdata->currentopstats[18]++;
			}
			dc->eptr->notifications = 1;
		}
	}
}

void matoclserv_notify_remove(uint32_t dirinode) {
	uint32_t hash = (dirinode*0x5F2318BD)%DIRINODE_HASH_SIZE;
	dirincache *dc;
	uint8_t *ptr;

	for (dc=dirinodehash[hash] ; dc ; dc=dc->nextnode) {
		if (dc->dirinode==dirinode) {
//                      syslog(LOG_NOTICE,"send to: '%s' ; removed inode: %" PRIu32,dc->eptr->sesdata->info,dirinode);
			ptr = matoclserv_createpacket(dc->eptr,MATOCL_FUSE_NOTIFY_REMOVE,8);
			stats_notify++;
			put32bit(&ptr,0);
			if (dirinode==dc->eptr->sesdata->rootinode) {
				put32bit(&ptr,MFS_ROOT_ID);
			} else {
				put32bit(&ptr,dirinode);
			}
			if (dc->eptr->sesdata) {
				dc->eptr->sesdata->currentopstats[19]++;
			}
			dc->eptr->notifications = 1;
		}
	}
}

void matoclserv_notify_parent(uint32_t dirinode,uint32_t parent) {
	uint32_t hash = (dirinode*0x5F2318BD)%DIRINODE_HASH_SIZE;
	dirincache *dc;
	uint8_t *ptr;

	for (dc=dirinodehash[hash] ; dc ; dc=dc->nextnode) {
		if (dc->dirinode==dirinode && dirinode!=dc->eptr->sesdata->rootinode) {
//                      syslog(LOG_NOTICE,"send to: '%s' ; new parent: %" PRIu32 "->%" PRIu32,dc->eptr->sesdata->info,dirinode,parent);
			ptr = matoclserv_createpacket(dc->eptr,MATOCL_FUSE_NOTIFY_PARENT,12);
			stats_notify++;
			put32bit(&ptr,0);
			put32bit(&ptr,dirinode);
			if (parent==dc->eptr->sesdata->rootinode) {
				put32bit(&ptr,MFS_ROOT_ID);
			} else {
				put32bit(&ptr,parent);
			}
			if (dc->eptr->sesdata) {
				dc->eptr->sesdata->currentopstats[20]++;
			}
			dc->eptr->notifications = 1;
		}
	}
}
*/

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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER - wrong size (%" PRIu32 "/<64)",length);
		eptr->mode = KILL;
		return;
	}
	tools = (memcmp(data,FUSE_REGISTER_BLOB_TOOLS_NOACL,64)==0)?1:0;
	if (eptr->registered == ClientState::kUnregistered
			&& (memcmp(data,FUSE_REGISTER_BLOB_NOACL,64)==0 || tools)) {
		if (RejectOld) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/NOACL - rejected (option REJECT_OLD_CLIENTS is set)");
			eptr->mode = KILL;
			return;
		}
		if (tools) {
			if (length!=64 && length!=68) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/NOACL-TOOLS - wrong size (%" PRIu32 "/64|68)",length);
				eptr->mode = KILL;
				return;
			}
		} else {
			if (length!=68 && length!=72) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/NOACL-MOUNT - wrong size (%" PRIu32 "/68|72)",length);
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
			syslog(LOG_NOTICE,"got register packet from mount older than 1.5 - rejecting");
			eptr->mode = KILL;
			return;
		}
		if (sessionid==0) {     // new session
			status = STATUS_OK; // exports_check(eptr->peerip,(const uint8_t*)"",NULL,NULL,&sesflags);      // check privileges for '/' w/o password
				eptr->sesdata = matoclserv_new_session(0,tools);
				if (eptr->sesdata==NULL) {
					syslog(LOG_NOTICE,"can't allocate session record");
					eptr->mode = KILL;
					return;
				}
				eptr->sesdata->rootinode = MFS_ROOT_ID;
				eptr->sesdata->sesflags = 0;
				eptr->sesdata->peerip = eptr->peerip;
		} else { // reconnect or tools
			eptr->sesdata = matoclserv_find_session(sessionid);
			if (eptr->sesdata==NULL) {      // in old model if session doesn't exist then create it
				eptr->sesdata = matoclserv_new_session(0,0);
				if (eptr->sesdata==NULL) {
					syslog(LOG_NOTICE,"can't allocate session record");
					eptr->mode = KILL;
					return;
				}
				eptr->sesdata->rootinode = MFS_ROOT_ID;
				eptr->sesdata->sesflags = 0;
				eptr->sesdata->peerip = eptr->peerip;
				status = STATUS_OK;
			} else if (eptr->sesdata->peerip==0) { // created by "filesystem"
				eptr->sesdata->peerip = eptr->peerip;
				status = STATUS_OK;
			} else if (eptr->sesdata->peerip==eptr->peerip) {
				status = STATUS_OK;
			} else {
				status = ERROR_EACCES;
			}
		}
		if (tools) {
			wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,1);
		} else {
			wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,(status!=STATUS_OK)?1:4);
		}
		if (status!=STATUS_OK) {
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
			syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL - wrong size (%" PRIu32 "/<65)",length);
			eptr->mode = KILL;
			return;
		}

		rptr = data+64;
		rcode = get8bit(&rptr);

		if ((eptr->registered == ClientState::kUnregistered && rcode==REGISTER_CLOSESESSION)
				|| (eptr->registered != ClientState::kUnregistered
						&& rcode!=REGISTER_CLOSESESSION)) {
			syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL - wrong rcode (%d) for registered status (%d)",rcode,eptr->registered);
			eptr->mode = KILL;
			return;
		}

		switch (rcode) {
		case REGISTER_GETRANDOM:
			if (length!=65) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.1 - wrong size (%" PRIu32 "/65)",length);
				eptr->mode = KILL;
				return;
			}
			wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,32);
			for (i=0 ; i<32 ; i++) {
				eptr->passwordrnd[i]=rndu8();
			}
			memcpy(wptr,eptr->passwordrnd,32);
			return;
		case REGISTER_NEWSESSION:
			if (length<77) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.2 - wrong size (%" PRIu32 "/>=77)",length);
				eptr->mode = KILL;
				return;
			}
			eptr->version = get32bit(&rptr);
			ileng = get32bit(&rptr);
			if (length<77+ileng) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.2 - wrong size (%" PRIu32 "/>=77+ileng(%" PRIu32 "))",length,ileng);
				eptr->mode = KILL;
				return;
			}
			info = (const char*)rptr;
			rptr+=ileng;
			pleng = get32bit(&rptr);
			if (length!=77+ileng+pleng && length!=77+16+ileng+pleng) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.2 - wrong size (%" PRIu32 "/77+ileng(%" PRIu32 ")+pleng(%" PRIu32 ")[+16])",length,ileng,pleng);
				eptr->mode = KILL;
				return;
			}
			path = rptr;
			rptr+=pleng;
			if (pleng>0 && rptr[-1]!=0) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.2 - received path without ending zero");
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
			if (status==STATUS_OK) {
				status = fs_getrootinode(&rootinode,path);
			}
			if (status==STATUS_OK) {
				eptr->sesdata = matoclserv_new_session(1,0);
				if (eptr->sesdata==NULL) {
					syslog(LOG_NOTICE,"can't allocate session record");
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
			wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,(status==STATUS_OK)?((eptr->version>=0x01061A)?35:(eptr->version>=0x010615)?25:(eptr->version>=0x010601)?21:13):1);
			if (status!=STATUS_OK) {
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
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.5 - wrong size (%" PRIu32 "/>=73)",length);
				eptr->mode = KILL;
				return;
			}
			eptr->version = get32bit(&rptr);
			ileng = get32bit(&rptr);
			if (length!=73+ileng && length!=73+16+ileng) {
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.5 - wrong size (%" PRIu32 "/73+ileng(%" PRIu32 ")[+16])",length,ileng);
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
			if (status==STATUS_OK) {
				eptr->sesdata = matoclserv_new_session(1,0);
				if (eptr->sesdata==NULL) {
					syslog(LOG_NOTICE,"can't allocate session record");
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
			wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,(status==STATUS_OK)?((eptr->version>=0x01061A)?19:(eptr->version>=0x010615)?9:5):1);
			if (status!=STATUS_OK) {
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
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.%" PRIu8 " - wrong size (%" PRIu32 "/73)",rcode,length);
				eptr->mode = KILL;
				return;
			}
			sessionid = get32bit(&rptr);
			eptr->version = get32bit(&rptr);
			eptr->sesdata = matoclserv_find_session(sessionid);
			if (eptr->sesdata == NULL || eptr->sesdata->peerip == 0) {
				status = ERROR_BADSESSIONID;
			} else {
				if ((eptr->sesdata->sesflags&SESFLAG_DYNAMICIP)==0 && eptr->peerip!=eptr->sesdata->peerip) {
					status = ERROR_EACCES;
				} else {
					status = STATUS_OK;
				}
			}
			wptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REGISTER,1);
			put8bit(&wptr,status);
			if (status!=STATUS_OK) {
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
				syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.6 - wrong size (%" PRIu32 "/69)",length);
				eptr->mode = KILL;
				return;
			}
			sessionid = get32bit(&rptr);
			matoclserv_close_session(sessionid);
			eptr->mode = KILL;
			return;
		}
		syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL - wrong rcode (%" PRIu8 ")",rcode);
		eptr->mode = KILL;
		return;
	} else {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_REGISTER - wrong register blob");
		eptr->mode = KILL;
		return;
	}
}

void matoclserv_fuse_reserved_inodes(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	const uint8_t *ptr;
	filelist *ofptr,**ofpptr;
	uint32_t inode;

	if ((length&0x3)!=0) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_RESERVED_INODES - wrong size (%" PRIu32 "/N*4)",length);
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

	FsContext context = FsContext::getForMaster(main_time());
	changelog_disable_flush();
	while ((ofptr=*ofpptr) && inode>0) {
		if (ofptr->inode<inode) {
			fs_release(context, ofptr->inode, eptr->sesdata->sessionid);
			*ofpptr = ofptr->next;
			free(ofptr);
		} else if (ofptr->inode>inode) {
			if (fs_acquire(context, inode, eptr->sesdata->sessionid) == STATUS_OK) {
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
		if (fs_acquire(context, inode, eptr->sesdata->sessionid) == STATUS_OK) {
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_STATFS - wrong size (%" PRIu32 "/4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	fs_statfs(eptr->sesdata->rootinode,eptr->sesdata->sesflags,&totalspace,&availspace,&trashspace,&reservedspace,&inodes);
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_ACCESS - wrong size (%" PRIu32 "/17)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	gid = get32bit(&data);
	matoclserv_ugid_remap(eptr,&uid,&gid);
	modemask = get8bit(&data);
	status = fs_access(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,uid,gid,modemask);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_ACCESS,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_lookup(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid,auid,agid;
	uint8_t nleng;
	const uint8_t *name;
	uint32_t newinode;
	Attributes attr;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<17) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_LOOKUP - wrong size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=17U+nleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_LOOKUP - wrong size (%" PRIu32 ":nleng=%" PRIu8 ")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	auid = uid = get32bit(&data);
	agid = gid = get32bit(&data);
	matoclserv_ugid_remap(eptr,&uid,&gid);
	status = fs_lookup(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,nleng,name,uid,gid,auid,agid,&newinode,attr);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_LOOKUP,(status!=STATUS_OK)?5:43);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,newinode);
		memcpy(ptr,attr,35);
	}
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[3]++;
	}
}

void matoclserv_fuse_getattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid,auid,agid;
	Attributes attr;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8 && length!=16) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETATTR - wrong size (%" PRIu32 "/8,16)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (length==16) {
		auid = uid = get32bit(&data);
		agid = gid = get32bit(&data);
		matoclserv_ugid_remap(eptr,&uid,&gid);
	} else {
		auid = uid = 12345;
		agid = gid = 12345;
	}
	status = fs_getattr(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,uid,gid,auid,agid,attr);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETATTR,(status!=STATUS_OK)?5:39);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		memcpy(ptr,attr,35);
	}
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[1]++;
	}
}

void matoclserv_fuse_setattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid,auid,agid;
	uint16_t setmask;
	Attributes attr;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	SugidClearMode sugidclearmode;
	uint16_t attrmode;
	uint32_t attruid,attrgid,attratime,attrmtime;
	if (length!=35 && length!=36) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETATTR - wrong size (%" PRIu32 "/35|36)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	auid = uid = get32bit(&data);
	agid = gid = get32bit(&data);
	matoclserv_ugid_remap(eptr,&uid,&gid);
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
	status = fs_setattr(eptr->sesdata->rootinode, eptr->sesdata->sesflags,
			inode, uid, gid, auid, agid,
			setmask, attrmode, attruid, attrgid, attratime, attrmtime, sugidclearmode, attr);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SETATTR,(status!=STATUS_OK)?5:39);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		memcpy(ptr,attr,35);
	}
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[2]++;
	}
}

void matoclserv_fuse_truncate(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid,auid,agid;
	Attributes attr;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t opened;
	uint8_t status;
	uint64_t attrlength;
	chunklist *cl;
	uint64_t chunkid;
	if (length!=24 && length!=25) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_TRUNCATE - wrong size (%" PRIu32 "/24|25)",length);
		eptr->mode = KILL;
		return;
	}
	opened = 0;
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (length==25) {
		opened = get8bit(&data);
	}
	auid = uid = get32bit(&data);
	agid = gid = get32bit(&data);
	if (length==24) {
		if (uid==0 && gid!=0) { // stupid "opened" patch for old clients
			opened = 1;
		}
	}
	matoclserv_ugid_remap(eptr,&uid,&gid);
	attrlength = get64bit(&data);
	status = fs_try_setlength(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,opened,uid,gid,auid,agid,attrlength,attr,&chunkid);
	if (status==ERROR_DELAYED) {
		cl = (chunklist*)malloc(sizeof(chunklist));
		passert(cl);
		cl->chunkid = chunkid;
		cl->qid = msgid;
		cl->inode = inode;
		cl->uid = uid;
		cl->gid = gid;
		cl->auid = auid;
		cl->agid = agid;
		cl->fleng = attrlength;
		cl->type = FUSE_TRUNCATE;
		cl->next = eptr->chunkdelayedops;
		eptr->chunkdelayedops = cl;
		if (eptr->sesdata) {
			eptr->sesdata->currentopstats[2]++;
		}
		return;
	}
	if (status==STATUS_OK) {
		status = fs_do_setlength(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,uid,gid,auid,agid,attrlength,attr);
	}
	if (status==STATUS_OK) {
		dcm_modify(inode,eptr->sesdata->sessionid);
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_TRUNCATE,(status!=STATUS_OK)?5:39);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		memcpy(ptr,attr,35);
	}
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[2]++;
	}
}

void matoclserv_fuse_readlink(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t pleng;
	uint8_t *path;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_READLINK - wrong size (%" PRIu32 "/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_readlink(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,&pleng,&path);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_READLINK,(status!=STATUS_OK)?5:8+pleng+1);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,pleng+1);
		if (pleng>0) {
			memcpy(ptr,path,pleng);
		}
		ptr[pleng]=0;
	}
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[7]++;
	}
}

void matoclserv_fuse_symlink(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint8_t nleng;
	const uint8_t *name,*path;
	uint32_t uid,gid;
	uint32_t pleng;
	uint32_t newinode;
	Attributes attr;
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (length<21) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SYMLINK - wrong size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length<21U+nleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SYMLINK - wrong size (%" PRIu32 ":nleng=%" PRIu8 ")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	pleng = get32bit(&data);
	if (length!=21U+nleng+pleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SYMLINK - wrong size (%" PRIu32 ":nleng=%" PRIu8 ":pleng=%" PRIu32 ")",length,nleng,pleng);
		eptr->mode = KILL;
		return;
	}
	path = data;
	data += pleng;
	uid = get32bit(&data);
	gid = get32bit(&data);
	while (pleng>0 && path[pleng-1]==0) {
		pleng--;
	}
	status = fs_symlink(matoclserv_get_context(eptr, uid, gid),inode,nleng,name,pleng,path,&newinode,&attr);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SYMLINK,(status!=STATUS_OK)?5:43);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,newinode);
		memcpy(ptr,attr,35);
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
	uint32_t auid = uid;
	uint32_t agid = gid;
	matoclserv_ugid_remap(eptr, &uid, &gid);

	uint32_t newinode;
	Attributes attr;
	uint8_t status = fs_mknod(eptr->sesdata->rootinode, eptr->sesdata->sesflags,
			inode, name.size(), reinterpret_cast<const uint8_t*>(name.data()),
			type, mode, umask, uid, gid, auid, agid, rdev, &newinode, attr);

	MessageBuffer reply;
	if (status == STATUS_OK && header.type == CLTOMA_FUSE_MKNOD) {
		serializeMooseFsPacket(reply, MATOCL_FUSE_MKNOD, messageId, newinode, attr);
	} else if (status == STATUS_OK && header.type == LIZ_CLTOMA_FUSE_MKNOD) {
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
	uint32_t auid = uid;
	uint32_t agid = gid;
	matoclserv_ugid_remap(eptr, &uid, &gid);

	uint32_t newinode;
	Attributes attr;
	uint8_t status = fs_mkdir(eptr->sesdata->rootinode, eptr->sesdata->sesflags,
			inode, name.size(), reinterpret_cast<const uint8_t*>(name.data()),
			mode, umask, uid, gid, auid, agid, copysgid, &newinode, attr);

	MessageBuffer reply;
	if (status == STATUS_OK && header.type == CLTOMA_FUSE_MKDIR) {
		serializeMooseFsPacket(reply, MATOCL_FUSE_MKDIR, messageId, newinode, attr);
	} else if (status == STATUS_OK && header.type == LIZ_CLTOMA_FUSE_MKDIR) {
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_UNLINK - wrong size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=17U+nleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_UNLINK - wrong size (%" PRIu32 ":nleng=%" PRIu8 ")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	uid = get32bit(&data);
	gid = get32bit(&data);
	matoclserv_ugid_remap(eptr,&uid,&gid);
	status = fs_unlink(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,nleng,name,uid,gid);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_UNLINK,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[9]++;
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_RMDIR - wrong size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=17U+nleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_RMDIR - wrong size (%" PRIu32 ":nleng=%" PRIu8 ")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	uid = get32bit(&data);
	gid = get32bit(&data);
	matoclserv_ugid_remap(eptr,&uid,&gid);
	status = fs_rmdir(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,nleng,name,uid,gid);
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_RENAME - wrong size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode_src = get32bit(&data);
	nleng_src = get8bit(&data);
	if (length<22U+nleng_src) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_RENAME - wrong size (%" PRIu32 ":nleng_src=%" PRIu8 ")",length,nleng_src);
		eptr->mode = KILL;
		return;
	}
	name_src = data;
	data += nleng_src;
	inode_dst = get32bit(&data);
	nleng_dst = get8bit(&data);
	if (length!=22U+nleng_src+nleng_dst) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_RENAME - wrong size (%" PRIu32 ":nleng_src=%" PRIu8 ":nleng_dst=%" PRIu8 ")",length,nleng_src,nleng_dst);
		eptr->mode = KILL;
		return;
	}
	name_dst = data;
	data += nleng_dst;
	uid = get32bit(&data);
	gid = get32bit(&data);
	status = fs_rename(matoclserv_get_context(eptr, uid, gid),
			inode_src, nleng_src, name_src, inode_dst, nleng_dst, name_dst, &inode, &attr);
	if (eptr->version>=0x010615 && status==STATUS_OK) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_RENAME,43);
	} else {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_RENAME,5);
	}
	put32bit(&ptr,msgid);
	if (eptr->version>=0x010615 && status==STATUS_OK) {
		put32bit(&ptr,inode);
		memcpy(ptr,attr,35);
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_LINK - wrong size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	inode_dst = get32bit(&data);
	nleng_dst = get8bit(&data);
	if (length!=21U+nleng_dst) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_LINK - wrong size (%" PRIu32 ":nleng_dst=%" PRIu8 ")",length,nleng_dst);
		eptr->mode = KILL;
		return;
	}
	name_dst = data;
	data += nleng_dst;
	uid = get32bit(&data);
	gid = get32bit(&data);
	status = fs_link(matoclserv_get_context(eptr, uid, gid),
			inode, inode_dst, nleng_dst, name_dst, &newinode, &attr);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_LINK,(status!=STATUS_OK)?5:43);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,newinode);
		memcpy(ptr,attr,35);
	}
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[11]++;
	}
}

void matoclserv_fuse_getdir(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid,auid,agid;
	uint8_t flags;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint32_t dleng;
	void *custom;
	if (length!=16 && length!=17) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETDIR - wrong size (%" PRIu32 "/16|17)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	auid = uid = get32bit(&data);
	agid = gid = get32bit(&data);
	matoclserv_ugid_remap(eptr,&uid,&gid);
	if (length==17) {
		flags = get8bit(&data);
	} else {
		flags = 0;
	}
	status = fs_readdir_size(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,uid,gid,flags,&custom,&dleng);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETDIR,(status!=STATUS_OK)?5:4+dleng);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		fs_readdir_data(eptr->sesdata->rootinode,eptr->sesdata->sesflags,uid,gid,auid,agid,flags,custom,ptr);
/* CACHENOTIFY
		if (flags&GETDIR_FLAG_ADDTOCACHE) {
			if (inode==MFS_ROOT_ID) {
				matoclserv_notify_add_dir(eptr,eptr->sesdata->rootinode);
			} else {
				matoclserv_notify_add_dir(eptr,inode);
			}
		}
*/
	}
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[12]++;
	}
}

/* CACHENOTIFY
void matoclserv_fuse_dir_removed(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	if (length%4!=0) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_DIR_REMOVED - wrong size (%" PRIu32 "/N*4)",length);
		eptr->mode = KILL;
		return;
	}
	if (get32bit(&data)) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_DIR_REMOVED - wrong msgid");
		eptr->mode = KILL;
		return;
	}
	length-=4;
	while (length) {
		inode = get32bit(&data);
		length-=4;
		if (inode==MFS_ROOT_ID) {
			matoclserv_notify_remove_dir(eptr,eptr->sesdata->rootinode);
		} else {
			matoclserv_notify_remove_dir(eptr,inode);
		}
	}
}
*/

void matoclserv_fuse_open(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid,auid,agid;
	uint8_t flags;
	Attributes attr;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	int allowcache;
	if (length!=17) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_OPEN - wrong size (%" PRIu32 "/17)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	auid = uid = get32bit(&data);
	agid = gid = get32bit(&data);
	matoclserv_ugid_remap(eptr,&uid,&gid);
	flags = get8bit(&data);
	status = matoclserv_insert_openfile(eptr->sesdata,inode);
	if (status==STATUS_OK) {
		status = fs_opencheck(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,uid,gid,auid,agid,flags,attr);
	}
	if (eptr->version>=0x010609 && status==STATUS_OK) {
		allowcache = dcm_open(inode,eptr->sesdata->sessionid);
		if (allowcache==0) {
			attr[1]&=(0xFF^(MATTR_ALLOWDATACACHE<<4));
		}
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_OPEN,39);
	} else {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_OPEN,5);
	}
	put32bit(&ptr,msgid);
	if (eptr->version>=0x010609 && status==STATUS_OK) {
		memcpy(ptr,attr,35);
	} else {
		put8bit(&ptr,status);
	}
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[13]++;
	}
}

void matoclserv_fuse_read_chunk(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t status;
	uint32_t inode;
	uint32_t indx;
	uint64_t chunkid;
	uint64_t fleng;
	uint32_t version;
	uint8_t count;
	uint8_t loc[100*6];
	uint32_t msgid;
	if (length!=12) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_READ_CHUNK - wrong size (%" PRIu32 "/12)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	indx = get32bit(&data);
	status = fs_readchunk(inode,indx,&chunkid,&fleng);
	if (status==STATUS_OK) {
		if (chunkid>0) {
			status = chunk_getversionandlocations(chunkid,eptr->peerip,&version,&count,loc);
		} else {
			version = 0;
			count = 0;
		}
	}
	if (status!=STATUS_OK) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_READ_CHUNK,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
		return;
	}
	dcm_access(inode,eptr->sesdata->sessionid);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_READ_CHUNK,24+count*6);
	put32bit(&ptr,msgid);
	put64bit(&ptr,fleng);
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	memcpy(ptr,loc,count*6);
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[14]++;
	}
}

void matoclserv_chunk_info(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint8_t status;
	uint64_t chunkid;
	uint64_t fleng;
	uint32_t version;
	uint32_t messageId;
	uint32_t inode;
	uint32_t index;
	std::vector<uint8_t> outMessage;

	cltoma::chunkInfo::deserialize(data, length, messageId, inode, index);

	status = fs_readchunk(inode, index, &chunkid, &fleng);
	std::vector<ChunkWithAddressAndLabel> allChunkCopies;
	if (status == STATUS_OK) {
		if (chunkid > 0) {
			status = chunk_getversionandlocations(chunkid, eptr->peerip, version,
					kMaxNumberOfChunkCopies, allChunkCopies);
		} else {
			version = 0;
		}
	}

	if (status != STATUS_OK) {
		matocl::chunkInfo::serialize(outMessage, messageId, status);
		matoclserv_createpacket(eptr, outMessage);
		return;
	}

	dcm_access(inode, eptr->sesdata->sessionid);
	matocl::chunkInfo::serialize(outMessage, messageId, fleng, chunkid, version, allChunkCopies);
	matoclserv_createpacket(eptr, outMessage);

	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[14]++;
	}
}

void matoclserv_fuse_write_chunk(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t status;
	uint32_t inode;
	uint32_t indx;
	uint64_t fleng;
	uint64_t chunkid;
	uint32_t msgid;
	uint8_t opflag;
	chunklist *cl;
	uint32_t version;
	uint8_t count;
	uint8_t loc[100*6];

	if (length!=12) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_WRITE_CHUNK - wrong size (%" PRIu32 "/12)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	indx = get32bit(&data);
	status = fs_writechunk(matoclserv_get_context(eptr), inode, indx, &chunkid, &opflag, &fleng);
	if (status!=STATUS_OK) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_WRITE_CHUNK,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
		return;
	}
	if (opflag) {   // wait for operation end
		cl = (chunklist*)malloc(sizeof(chunklist));
		passert(cl);
		cl->inode = inode;
		cl->chunkid = chunkid;
		cl->qid = msgid;
		cl->fleng = fleng;
		cl->type = FUSE_WRITE;
		cl->next = eptr->chunkdelayedops;
		eptr->chunkdelayedops = cl;
	} else {        // return status immediately
		dcm_modify(inode,eptr->sesdata->sessionid);
		status=chunk_getversionandlocations(chunkid,eptr->peerip,&version,&count,loc);
		if (status!=STATUS_OK) {
			ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_WRITE_CHUNK,5);
			put32bit(&ptr,msgid);
			put8bit(&ptr,status);
			fs_writeend(0,0,chunkid);       // ignore status - just do it.
			return;
		}
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_WRITE_CHUNK,24+count*6);
		put32bit(&ptr,msgid);
		put64bit(&ptr,fleng);
		put64bit(&ptr,chunkid);
		put32bit(&ptr,version);
		memcpy(ptr,loc,count*6);
	}
	if (eptr->sesdata) {
		eptr->sesdata->currentopstats[15]++;
	}
}

void matoclserv_fuse_write_chunk_end(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint32_t msgid;
	uint32_t inode;
	uint64_t fleng;
	uint64_t chunkid;
	uint8_t status;
	if (length!=24) {
		syslog(LOG_NOTICE,"CLTOMA_WRITE_CHUNK_END - wrong size (%" PRIu32 "/24)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	chunkid = get64bit(&data);
	inode = get32bit(&data);
	fleng = get64bit(&data);
	if (eptr->sesdata->sesflags&SESFLAG_READONLY) {
		status = ERROR_EROFS;
	} else {
		status = fs_writeend(inode,fleng,chunkid);
	}
	dcm_modify(inode,eptr->sesdata->sessionid);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_WRITE_CHUNK_END,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_repair(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint32_t msgid;
	uint32_t chunksnotchanged,chunkserased,chunksrepaired;
	uint8_t *ptr;
	uint8_t status;
	if (length!=16) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_REPAIR - wrong size (%" PRIu32 "/16)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	gid = get32bit(&data);
	matoclserv_ugid_remap(eptr,&uid,&gid);
	status = fs_repair(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,uid,gid,&chunksnotchanged,&chunkserased,&chunksrepaired);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_REPAIR,(status!=STATUS_OK)?5:16);
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_CHECK - wrong size (%" PRIu32 "/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_checkfile(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,chunkcount);
	if (status!=STATUS_OK) {
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


void matoclserv_fuse_gettrashtime(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint8_t gmode;
	void *fptr,*dptr;
	uint32_t fnodes,dnodes;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=9) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETTRASHTIME - wrong size (%" PRIu32 "/9)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	gmode = get8bit(&data);
	status = fs_gettrashtime_prepare(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,gmode,&fptr,&dptr,&fnodes,&dnodes);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETTRASHTIME,(status!=STATUS_OK)?5:12+8*(fnodes+dnodes));
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,fnodes);
		put32bit(&ptr,dnodes);
		fs_gettrashtime_store(fptr,dptr,ptr);
	}
}

void matoclserv_fuse_settrashtime(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,trashtime;
	uint32_t msgid;
	uint8_t smode;
	uint32_t changed,notchanged,notpermitted;
	uint8_t *ptr;
	uint8_t status;
	if (length!=17) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETTRASHTIME - wrong size (%" PRIu32 "/17)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	trashtime = get32bit(&data);
	smode = get8bit(&data);
// limits check
	status = STATUS_OK;
	switch (smode&SMODE_TMASK) {
	case SMODE_SET:
		if (trashtime<eptr->sesdata->mintrashtime || trashtime>eptr->sesdata->maxtrashtime) {
			status = ERROR_EPERM;
		}
		break;
	case SMODE_INCREASE:
		if (trashtime>eptr->sesdata->maxtrashtime) {
			status = ERROR_EPERM;
		}
		break;
	case SMODE_DECREASE:
		if (trashtime<eptr->sesdata->mintrashtime) {
			status = ERROR_EPERM;
		}
		break;
	}

	if (status==STATUS_OK) {
		status = fs_settrashtime(matoclserv_get_context(eptr, uid, 0), inode, trashtime, smode, &changed, &notchanged, &notpermitted);
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SETTRASHTIME,(status!=STATUS_OK)?5:16);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,changed);
		put32bit(&ptr,notchanged);
		put32bit(&ptr,notpermitted);
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

	GoalMap<uint32_t> fgtab, dgtab;
	uint8_t status = fs_getgoal(eptr->sesdata->rootinode, eptr->sesdata->sesflags,
			inode, gmode, fgtab, dgtab);

	MessageBuffer reply;
	if (status == STATUS_OK) {
		GoalMap<Goal> goalDefinitions = fs_get_goal_definitions();
		std::vector<FuseGetGoalStats> lizReply;
		MooseFSVector<std::pair<uint8_t, uint32_t>> mooseFsReplyFiles, mooseFsReplyDirectories;
		for (uint8_t goal = goal::kMinGoal; goal <= goal::kMaxGoal; goal++) {
			if (fgtab[goal] || dgtab[goal]) {
				lizReply.emplace_back(goalDefinitions[goal].name(), fgtab[goal], dgtab[goal]);
			}
			if (fgtab[goal] > 0) {
				mooseFsReplyFiles.emplace_back(goal, fgtab[goal]);
			}
			if (dgtab[goal] > 0) {
				mooseFsReplyDirectories.emplace_back(goal, dgtab[goal]);
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

void matoclserv_fuse_setgoal(matoclserventry *eptr, PacketHeader header, const uint8_t *data) {
	uint32_t inode, uid, msgid;
	uint8_t goalId, smode;
	uint8_t status = STATUS_OK;

	if (header.type == CLTOMA_FUSE_SETGOAL) {
		deserializeAllMooseFsPacketDataNoHeader(data, header.length,
				msgid, inode, uid, goalId, smode);
	} else if (header.type == LIZ_CLTOMA_FUSE_SETGOAL) {
		std::string goalName;
		cltoma::fuseSetGoal::deserialize(data, header.length,
				msgid, inode, uid, goalName, smode);
		// find a proper goalId,
		GoalMap<Goal> goalDefinitions = fs_get_goal_definitions();
		bool goalFound = false;
		for (goalId = goal::kMinGoal; goalId <= goal::kMaxGoal; goalId++) {
			if (goalDefinitions[goalId].name() == goalName) {
				goalFound = true;
				break;
			}
		}
		if (!goalFound) {
			status = ERROR_EINVAL;
		}
	} else {
		throw IncorrectDeserializationException(
				"Unknown packet type for matoclserv_fuse_getgoal: " + std::to_string(header.type));
	}

	uint8_t smodeType = smode & SMODE_TMASK;
	if (status == STATUS_OK && smodeType != SMODE_INCREASE && goalId < eptr->sesdata->mingoal) {
		status = ERROR_EPERM;
	}
	if (status == STATUS_OK && smodeType != SMODE_DECREASE && goalId > eptr->sesdata->maxgoal) {
		status = ERROR_EPERM;
	}
	if (status == STATUS_OK && !goal::isGoalValid(goalId)) {
		status = ERROR_EINVAL;
	}

	uint32_t changed,notchanged,notpermitted;
	if (status == STATUS_OK) {
		status = fs_setgoal(matoclserv_get_context(eptr, uid, 0), inode, goalId, smode,
				&changed, &notchanged, &notpermitted);
	}

	MessageBuffer reply;
	if (status == STATUS_OK) {
		if (header.type == LIZ_CLTOMA_FUSE_SETGOAL) {
			matocl::fuseSetGoal::serialize(reply, msgid, changed, notchanged, notpermitted);
		} else {
			serializeMooseFsPacket(reply, MATOCL_FUSE_SETGOAL,
					msgid, changed, notchanged, notpermitted);
		}
	} else {
		if (header.type == LIZ_CLTOMA_FUSE_SETGOAL) {
			matocl::fuseSetGoal::serialize(reply, msgid, status);
		} else {
			serializeMooseFsPacket(reply, MATOCL_FUSE_SETGOAL, msgid, status);
		}
	}
	matoclserv_createpacket(eptr, std::move(reply));
}

void matoclserv_fuse_geteattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint32_t feattrtab[16],deattrtab[16];
	uint8_t i,fn,dn,gmode;
	uint8_t *ptr;
	uint8_t status;
	if (length!=9) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETEATTR - wrong size (%" PRIu32 "/9)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	gmode = get8bit(&data);
	status = fs_geteattr(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,gmode,feattrtab,deattrtab);
	fn=0;
	dn=0;
	if (status==STATUS_OK) {
		for (i=0 ; i<16 ; i++) {
			if (feattrtab[i]) {
				fn++;
			}
			if (deattrtab[i]) {
				dn++;
			}
		}
	}
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETEATTR,(status!=STATUS_OK)?5:6+5*(fn+dn));
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETEATTR - wrong size (%" PRIu32 "/14)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	eattr = get8bit(&data);
	smode = get8bit(&data);
	status = fs_seteattr(matoclserv_get_context(eptr, uid, 0), inode, eattr, smode, &changed, &notchanged, &notpermitted);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SETEATTR,(status!=STATUS_OK)?5:16);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETXATTR - wrong size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	opened = get8bit(&data);
	uid = get32bit(&data);
	gid = get32bit(&data);
	matoclserv_ugid_remap(eptr,&uid,&gid);
	anleng = get8bit(&data);
	attrname = data;
	data+=anleng;
	if (length!=19U+anleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETXATTR - wrong size (%" PRIu32 ":anleng=%" PRIu8 ")",length,anleng);
		eptr->mode = KILL;
		return;
	}
	mode = get8bit(&data);
	if (mode!=XATTR_GMODE_GET_DATA && mode!=XATTR_GMODE_LENGTH_ONLY) {
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETXATTR,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,ERROR_EINVAL);
	} else if (anleng==0) {
		void *xanode;
		uint32_t xasize;
		status = fs_listxattr_leng(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,opened,uid,gid,&xanode,&xasize);
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETXATTR,(status!=STATUS_OK)?5:8+((mode==XATTR_GMODE_GET_DATA)?xasize:0));
		put32bit(&ptr,msgid);
		if (status!=STATUS_OK) {
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
		status = fs_getxattr(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,opened,uid,gid,anleng,attrname,&avleng,&attrvalue);
		ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETXATTR,(status!=STATUS_OK)?5:8+((mode==XATTR_GMODE_GET_DATA)?avleng:0));
		put32bit(&ptr,msgid);
		if (status!=STATUS_OK) {
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETXATTR - wrong size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	opened = get8bit(&data);
	uid = get32bit(&data);
	gid = get32bit(&data);
	matoclserv_ugid_remap(eptr,&uid,&gid);
	anleng = get8bit(&data);
	if (length<23U+anleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETXATTR - wrong size (%" PRIu32 ":anleng=%" PRIu8 ")",length,anleng);
		eptr->mode = KILL;
		return;
	}
	attrname = data;
	data += anleng;
	avleng = get32bit(&data);
	if (length!=23U+anleng+avleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETXATTR - wrong size (%" PRIu32 ":anleng=%" PRIu8 ":avleng=%" PRIu32 ")",length,anleng,avleng);
		eptr->mode = KILL;
		return;
	}
	attrvalue = data;
	data += avleng;
	mode = get8bit(&data);
	status = fs_setxattr(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,opened,uid,gid,anleng,attrname,avleng,attrvalue,mode);
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_APPEND - wrong size (%" PRIu32 "/20)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	inode_src = get32bit(&data);
	uid = get32bit(&data);
	gid = get32bit(&data);
	status = fs_append(matoclserv_get_context(eptr, uid, gid), inode, inode_src);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_APPEND,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_snapshot(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inode_dst;
	uint8_t nleng_dst;
	const uint8_t *name_dst;
	uint32_t uid,gid;
	uint8_t canoverwrite;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<22) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SNAPSHOT - wrong size (%" PRIu32 ")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	inode_dst = get32bit(&data);
	nleng_dst = get8bit(&data);
	if (length!=22U+nleng_dst) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SNAPSHOT - wrong size (%" PRIu32 ":nleng_dst=%" PRIu8 ")",length,nleng_dst);
		eptr->mode = KILL;
		return;
	}
	name_dst = data;
	data += nleng_dst;
	uid = get32bit(&data);
	gid = get32bit(&data);
	canoverwrite = get8bit(&data);
	status = fs_snapshot(matoclserv_get_context(eptr, uid, gid),
			inode, inode_dst, nleng_dst, name_dst, canoverwrite);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_SNAPSHOT,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_getdirstats_old(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inodes,files,dirs,chunks;
	uint64_t leng,size,rsize;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETDIRSTATS - wrong size (%" PRIu32 "/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_get_dir_stats(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,&inodes,&dirs,&files,&chunks,&leng,&size,&rsize);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETDIRSTATS,(status!=STATUS_OK)?5:60);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETDIRSTATS - wrong size (%" PRIu32 "/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_get_dir_stats(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,&inodes,&dirs,&files,&chunks,&leng,&size,&rsize);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETDIRSTATS,(status!=STATUS_OK)?5:44);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETTRASH - wrong size (%" PRIu32 "/4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	status = fs_readtrash_size(eptr->sesdata->rootinode,eptr->sesdata->sesflags,&dleng);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETTRASH,(status!=STATUS_OK)?5:(4+dleng));
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		fs_readtrash_data(eptr->sesdata->rootinode,eptr->sesdata->sesflags,ptr);
	}
}

void matoclserv_fuse_getdetachedattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	Attributes attr;
	uint32_t msgid;
	uint8_t dtype;
	uint8_t *ptr;
	uint8_t status;
	if (length<8 || length>9) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETDETACHEDATTR - wrong size (%" PRIu32 "/8,9)",length);
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
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETDETACHEDATTR,(status!=STATUS_OK)?5:39);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		memcpy(ptr,attr,35);
	}
}

void matoclserv_fuse_gettrashpath(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t pleng;
	uint8_t *path;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETTRASHPATH - wrong size (%" PRIu32 "/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_gettrashpath(eptr->sesdata->rootinode,eptr->sesdata->sesflags,inode,&pleng,&path);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETTRASHPATH,(status!=STATUS_OK)?5:8+pleng+1);
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,pleng+1);
		if (pleng>0) {
			memcpy(ptr,path,pleng);
		}
		ptr[pleng]=0;
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETTRASHPATH - wrong size (%" PRIu32 "/>=12)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	pleng = get32bit(&data);
	if (length!=12+pleng) {
		syslog(LOG_NOTICE,"CLTOMA_FUSE_SETTRASHPATH - wrong size (%" PRIu32 "/%" PRIu32 ")",length,12+pleng);
		eptr->mode = KILL;
		return;
	}
	path = data;
	data += pleng;
	while (pleng>0 && path[pleng-1]==0) {
		pleng--;
	}
	status = fs_settrashpath(matoclserv_get_context(eptr), inode, pleng, path);
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_UNDEL - wrong size (%" PRIu32 "/8)",length);
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_PURGE - wrong size (%" PRIu32 "/8)",length);
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
		syslog(LOG_NOTICE,"CLTOMA_FUSE_GETRESERVED - wrong size (%" PRIu32 "/4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	status = fs_readreserved_size(eptr->sesdata->rootinode,eptr->sesdata->sesflags,&dleng);
	ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_GETRESERVED,(status!=STATUS_OK)?5:(4+dleng));
	put32bit(&ptr,msgid);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		fs_readreserved_data(eptr->sesdata->rootinode,eptr->sesdata->sesflags,ptr);
	}
}

void matoclserv_fuse_deleteacl(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t messageId, inode, uid, gid;
	AclType type;
	cltoma::fuseDeleteAcl::deserialize(data, length, messageId, inode, uid, gid, type);

	MessageBuffer reply;
	uint8_t status = fs_deleteacl(matoclserv_get_context(eptr, uid, gid), inode, type);
	matocl::fuseDeleteAcl::serialize(reply, messageId, status);
	matoclserv_createpacket(eptr, std::move(reply));
}

void matoclserv_fuse_getacl(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t messageId, inode, uid, gid;
	AclType type;
	cltoma::fuseGetAcl::deserialize(data, length, messageId, inode, uid, gid, type);
	DEBUG_LOG("master.cltoma_fuse_getacl") << inode;

	MessageBuffer reply;
	AccessControlList acl;
	uint8_t status = fs_getacl(matoclserv_get_context(eptr, uid, gid), inode, type, acl);
	if (status == STATUS_OK) {
		matocl::fuseGetAcl::serialize(reply, messageId, acl);
	} else {
		matocl::fuseGetAcl::serialize(reply, messageId, status);
	}
	matoclserv_createpacket(eptr, std::move(reply));
}

void matoclserv_fuse_setacl(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t messageId, inode, uid, gid;
	AclType type;
	AccessControlList acl;
	cltoma::fuseSetAcl::deserialize(data, length, messageId, inode, uid, gid, type, acl);

	MessageBuffer reply;
	uint8_t status = fs_setacl(matoclserv_get_context(eptr, uid, gid), inode, type, std::move(acl));
	matocl::fuseSetAcl::serialize(reply, messageId, status);
	matoclserv_createpacket(eptr, std::move(reply));
}

void matoclserv_fuse_setquota(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t messageId, uid, gid;
	std::vector<QuotaEntry> entries;
	cltoma::fuseSetQuota::deserialize(data, length, messageId, uid, gid, entries);
	matoclserv_ugid_remap(eptr, &uid, NULL);
	uint8_t status = fs_quota_set(eptr->sesdata->sesflags, uid, entries);
	MessageBuffer reply;
	matocl::fuseSetQuota::serialize(reply, messageId, status);
	matoclserv_createpacket(eptr, std::move(reply));
}

void matoclserv_fuse_getquota(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t version, messageId, uid, gid;
	std::vector<QuotaOwnerAndLimits> results;
	uint8_t status;
	deserializePacketVersionNoHeader(data, length, version);
	if (version == cltoma::fuseGetQuota::kAllLimits) {
		cltoma::fuseGetQuota::deserialize(data, length, messageId, uid, gid);
		matoclserv_ugid_remap(eptr, &uid, &gid);
		status = fs_quota_get_all(eptr->sesdata->sesflags, uid, results);
	} else if (version == cltoma::fuseGetQuota::kSelectedLimits) {
		std::vector<QuotaOwner> owners;
		cltoma::fuseGetQuota::deserialize(data, length, messageId, uid, gid, owners);
		matoclserv_ugid_remap(eptr, &uid, &gid);
		status = fs_quota_get(eptr->sesdata->sesflags, uid, gid, owners, results);
	} else {
		throw IncorrectDeserializationException(
				"Unknown LIZ_CLTOMA_FUSE_GET_QUOTA version: " + std::to_string(version));
	}
	MessageBuffer reply;
	if (status == STATUS_OK) {
		matocl::fuseGetQuota::serialize(reply, messageId, results);
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
			syslog(LOG_NOTICE, "LIZ_CLTOMA_IOLIMIT: Invalid group: %s", groupId.c_str());
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
			array[i] = rndu8();
		}
		matoclserv_createpacket(eptr, matocl::adminRegisterChallenge::build(array));
	} else {
		syslog(LOG_NOTICE, "LIZ_CLTOMA_ADMIN_REGISTER_CHALLENGE: retry not allowed");
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
			matoclserv_createpacket(eptr, matocl::adminRegisterResponse::build(ERROR_EPERM));
			syslog(LOG_WARNING, "admin access disabled");
			return;
		}
		auto digest = md5_challenge_response(*eptr->adminChallenge, password);
		if (receivedDigest == digest) {
			matoclserv_createpacket(eptr, matocl::adminRegisterResponse::build(STATUS_OK));
			eptr->registered = ClientState::kAdmin;
		} else {
			matoclserv_createpacket(eptr, matocl::adminRegisterResponse::build(ERROR_BADPASSWORD));
			syslog(LOG_WARNING, "admin authentication error");
		}
		eptr->adminChallenge.reset();
	} else {
		syslog(LOG_NOTICE,
				"LIZ_CLTOMA_ADMIN_REGISTER_RESPONSE: response without previous challenge");
		eptr->mode = KILL;
	}
}

void matoclserv_admin_become_master(matoclserventry* eptr, const uint8_t* data, uint32_t length) {
	cltoma::adminBecomeMaster::deserialize(data, length);
	if (eptr->registered == ClientState::kAdmin) {
		bool succ = metadataserver::promoteAutoToMaster();
		uint8_t status = succ ? STATUS_OK : ERROR_NOTPOSSIBLE;
		matoclserv_createpacket(eptr, matocl::adminBecomeMaster::build(status));
	} else {
		syslog(LOG_NOTICE,
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
				syslog(LOG_WARNING, "LIZ_CLTOMA_ADMIN_STOP_WITHOUT_METADATA_DUMP: Trying to stop"
						" master server with disabled metadata dump when no shadow servers are "
						"connected.");
				matoclserv_createpacket(eptr, matocl::adminStopWithoutMetadataDump::build(EPERM));
			} else {
				fs_disable_metadata_dump_on_exit();
				uint8_t status = main_want_to_terminate();
				if (status == STATUS_OK) {
					eptr->requestingTermination = true;
				} else {
					matoclserv_createpacket(
							eptr, matocl::adminStopWithoutMetadataDump::build(status));
				}
			}
		} else { // not Master
			fs_disable_metadata_dump_on_exit();
			uint8_t status = main_want_to_terminate();
			matoclserv_createpacket(eptr, matocl::adminStopWithoutMetadataDump::build(status));
		}
	} else {
		syslog(LOG_NOTICE,
						"LIZ_CLTOMA_ADMIN_STOP_WITHOUT_METADATA_DUMP:"
						" available only for registered admins");
		eptr->mode = KILL;
	}
}

void matocl_session_timedout(session *sesdata) {
	filelist *fl,*afl;
	fl=sesdata->openedfiles;
	FsContext context = FsContext::getForMaster(main_time());
	while (fl) {
		afl = fl;
		fl=fl->next;
		fs_release(context, afl->inode, sesdata->sessionid);
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

	now = main_time();
	sesdata = &(sessionshead);
	while ((asesdata=*sesdata)) {
//              syslog(LOG_NOTICE,"session: %u ; nsocks: %u ; state: %u ; disconnected: %u",asesdata->sessionid,asesdata->nsocks,asesdata->newsession,asesdata->disconnected);
		if (asesdata->nsocks==0 && ((asesdata->newsession>1 && asesdata->disconnected<now) || (asesdata->newsession==1 && asesdata->disconnected+SessionSustainTime<now) || (asesdata->newsession==0 && asesdata->disconnected+7200<now))) {
//                      syslog(LOG_NOTICE,"remove session: %u",asesdata->sessionid);
			matocl_session_timedout(asesdata);
			*sesdata = asesdata->next;
			free(asesdata);
		} else {
			sesdata = &(asesdata->next);
		}
	}
}

void matocl_session_statsmove(void) {
	session *sesdata;
	for (sesdata = sessionshead ; sesdata ; sesdata=sesdata->next) {
		memcpy(sesdata->lasthouropstats,sesdata->currentopstats,4*SESSION_STATS);
		memset(sesdata->currentopstats,0,4*SESSION_STATS);
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
			eptr->sesdata->disconnected = main_time();
		}
	}
/* CACHENOTIFY
	matoclserv_notify_disconnected(eptr);
*/
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
	try {
		if (!metadataserver::isMaster()) {     // shadow
			switch (type) {
				case LIZ_CLTOMA_METADATASERVER_STATUS:
					matoclserv_metadataserver_status(eptr, data, length);
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
				default:
					syslog(LOG_NOTICE,"main master server module: got invalid message in shadow state (type:%" PRIu32 ")",type);
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
					matoclserv_liz_cserv_list(eptr);
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
				default:
					syslog(LOG_NOTICE,"main master server module: got unknown message from unregistered (type:%" PRIu32 ")",type);
					eptr->mode=KILL;
			}
		} else if (eptr->registered == ClientState::kRegistered) {      // mounts and new tools
			if (eptr->sesdata==NULL) {
				syslog(LOG_ERR,"registered connection without sesdata !!!");
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
					/* CACHENOTIFY
					   case CLTOMA_FUSE_DIR_REMOVED:
					   matoclserv_fuse_dir_removed(eptr,data,length);
					   break;
					   */
				case CLTOMA_FUSE_OPEN:
					matoclserv_fuse_open(eptr,data,length);
					break;
				case CLTOMA_FUSE_READ_CHUNK:
					matoclserv_fuse_read_chunk(eptr,data,length);
					break;
				case LIZ_CLTOMA_CHUNK_INFO:
					matoclserv_chunk_info(eptr, data, length);
					break;
				case CLTOMA_FUSE_WRITE_CHUNK:
					matoclserv_fuse_write_chunk(eptr,data,length);
					break;
				case CLTOMA_FUSE_WRITE_CHUNK_END:
					matoclserv_fuse_write_chunk_end(eptr,data,length);
					break;
					// fuse - meta
				case CLTOMA_FUSE_GETTRASH:
					matoclserv_fuse_gettrash(eptr,data,length);
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
				case CLTOMA_FUSE_CHECK:
					matoclserv_fuse_check(eptr,data,length);
					break;
				case CLTOMA_FUSE_GETTRASHTIME:
					matoclserv_fuse_gettrashtime(eptr,data,length);
					break;
				case CLTOMA_FUSE_SETTRASHTIME:
					matoclserv_fuse_settrashtime(eptr,data,length);
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
				case CLTOMA_FUSE_TRUNCATE:
					matoclserv_fuse_truncate(eptr,data,length);
					break;
				case CLTOMA_FUSE_REPAIR:
					matoclserv_fuse_repair(eptr,data,length);
					break;
				case CLTOMA_FUSE_SNAPSHOT:
					matoclserv_fuse_snapshot(eptr,data,length);
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
				default:
					syslog(LOG_NOTICE,"main master server module: got unknown message from mfsmount (type:%" PRIu32 ")",type);
					eptr->mode=KILL;
			}
		} else if (eptr->registered == ClientState::kOldTools) {        // old mfstools
			if (eptr->sesdata==NULL) {
				syslog(LOG_ERR,"registered connection (tools) without sesdata !!!");
				eptr->mode=KILL;
				return;
			}
			switch (type) {
				// extra (external tools)
				case CLTOMA_FUSE_REGISTER:
					matoclserv_fuse_register(eptr,data,length);
					break;
				case CLTOMA_FUSE_READ_CHUNK:    // used in mfsfileinfo
					matoclserv_fuse_read_chunk(eptr,data,length);
					break;
				case CLTOMA_FUSE_CHECK:
					matoclserv_fuse_check(eptr,data,length);
					break;
				case CLTOMA_FUSE_GETTRASHTIME:
					matoclserv_fuse_gettrashtime(eptr,data,length);
					break;
				case CLTOMA_FUSE_SETTRASHTIME:
					matoclserv_fuse_settrashtime(eptr,data,length);
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
					matoclserv_fuse_truncate(eptr,data,length);
					break;
				case CLTOMA_FUSE_REPAIR:
					matoclserv_fuse_repair(eptr,data,length);
					break;
				case CLTOMA_FUSE_SNAPSHOT:
					matoclserv_fuse_snapshot(eptr,data,length);
					break;
				case CLTOMA_FUSE_GETEATTR:
					matoclserv_fuse_geteattr(eptr,data,length);
					break;
				case CLTOMA_FUSE_SETEATTR:
					matoclserv_fuse_seteattr(eptr,data,length);
					break;
				default:
					syslog(LOG_NOTICE,"main master server module: got unknown message from mfstools (type:%" PRIu32 ")",type);
					eptr->mode=KILL;
			}
		}
	} catch (IncorrectDeserializationException& e) {
		syslog(LOG_NOTICE,
				"main master server module: got inconsistent message from mount "
				"(type:%" PRIu32 ", length:%" PRIu32"), %s", type, length, e.what());
		eptr->mode = KILL;
	}
}

void matoclserv_term(void) {
	matoclserventry *eptr,*eptrn;
	packetstruct *pptr,*pptrn;
	chunklist *cl,*cln;

	syslog(LOG_NOTICE,"main master server module: closing %s:%s",ListenHost,ListenPort);
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
	int32_t i;
	uint32_t type,size;
	const uint8_t *ptr;
	for (;;) {
		i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
		if (i==0) {
			if (eptr->registered == ClientState::kRegistered) {       // show this message only for standard, registered clients
				syslog(LOG_NOTICE,"connection with client(ip:%u.%u.%u.%u) has been closed by peer",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
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
					syslog(LOG_WARNING,"main master server module: packet too long (%" PRIu32 "/%u)",size,MaxPacketSize);
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
		}
	}
}

void matoclserv_write(matoclserventry *eptr) {
	packetstruct *pack;
	int32_t i;
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
	}
}

void matoclserv_wantexit(void) {
	exiting=1;
}

int matoclserv_canexit(void) {
	matoclserventry *eptr;
	matoclserventry *adminTerminator = NULL;
	static bool terminatorPacketSent = false;
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		if (eptr->outputhead!=NULL) {
			return 0;
		}
		if (eptr->chunkdelayedops!=NULL) {
			return 0;
		}
		if (eptr->requestingTermination){
			adminTerminator = eptr;
		}
	}
	if (adminTerminator != NULL && !terminatorPacketSent) {
	// Are we replying to termination request?
		if (!matomlserv_canexit()){  // make sure there are no ml connected
			syslog(LOG_INFO, "Waiting for ml connections to close...");
			return 0;
		} else {  // Reply to admin
			matoclserv_createpacket(adminTerminator,
					matocl::adminStopWithoutMetadataDump::build(STATUS_OK));
			terminatorPacketSent = true;
		}
	}
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		if (eptr->requestingTermination){
		// Wait for the terminating admin to disconnect - this ensures that he receives data
			return 0;
		}
	}
	return 1;
}

void matoclserv_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	matoclserventry *eptr;

	if (exiting==0) {
		pdesc[pos].fd = lsock;
		pdesc[pos].events = POLLIN;
		lsockpdescpos = pos;
		pos++;
	} else {
		lsockpdescpos = -1;
	}
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		pdesc[pos].fd = eptr->sock;
		pdesc[pos].events = 0;
		eptr->pdescpos = pos;
		if (exiting==0) {
			pdesc[pos].events |= POLLIN;
		}
		if (eptr->outputhead!=NULL) {
			pdesc[pos].events |= POLLOUT;
		}
		pos++;
	}
	*ndesc = pos;
}


void matoclserv_serve(struct pollfd *pdesc) {
	uint32_t now=main_time();
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
/* CACHENOTIFY
			eptr->notifications = 0;
*/
			eptr->version = 0;
			eptr->mode = HEADER;
			eptr->lastread = now;
			eptr->lastwrite = now;
			eptr->inputpacket.next = NULL;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;
			eptr->inputpacket.packet = NULL;
			eptr->requestingTermination = false;
			eptr->outputhead = NULL;
			eptr->outputtail = &(eptr->outputhead);

			eptr->chunkdelayedops = NULL;
			eptr->sesdata = NULL;
/* CACHENOTIFY
			eptr->cacheddirs = NULL;
*/
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
/* CACHENOTIFY
			if (eptr->notifications) {
				if (eptr->version>=0x010616) {
					uint8_t *ptr = matoclserv_createpacket(eptr,MATOCL_FUSE_NOTIFY_END,4);  // transaction end
					*((uint32_t*)ptr) = 0;
				}
				eptr->notifications = 0;
			}
*/
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
		main_timeregister(TIMEMODE_RUN_LATE,1,0,matoclserv_start_cond_check);
	}
	main_timeregister(TIMEMODE_RUN_LATE,10,0,matocl_session_check);
	main_timeregister(TIMEMODE_RUN_LATE,3600,0,matocl_session_statsmove);
	return;
}

void matoclserv_reload(void) {
	char *oldListenHost,*oldListenPort;
	int newlsock;

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

	oldListenHost = ListenHost;
	oldListenPort = ListenPort;
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

	newlsock = tcpsocket();
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
/* CACHENOTIFY
	matoclserv_dircache_init();
*/

	if (metadataserver::isMaster()) {
		matoclserv_become_master();
	}
	main_reloadregister(matoclserv_reload);
	metadataserver::registerFunctionCalledOnPromotion(matoclserv_become_master);
	main_destructregister(matoclserv_term);
	main_pollregister(matoclserv_desc,matoclserv_serve);
	main_wantexitregister(matoclserv_wantexit);
	main_canexitregister(matoclserv_canexit);
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
		free(ss);
	}
	sessionshead = nullptr;
}

