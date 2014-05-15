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
#include "master/filesystem.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <syslog.h>
#include <unistd.h>
#include <memory>
#include <string>
#include <vector>

#include "common/cwrap.h"
#include "common/datapack.h"
#include "common/exceptions.h"
#include "common/hashfn.h"
#include "common/lizardfs_version.h"
#include "common/massert.h"
#include "common/metadata.h"
#include "common/MFSCommunication.h"
#include "common/slogger.h"
#include "master/checksum.h"
#include "master/chunks.h"
#include "master/metadata_dumper.h"
#include "master/quota_database.h"

#ifdef HAVE_PWD_H
#  include <pwd.h>
#endif
#ifndef METARESTORE
#  include "common/cfg.h"
#  include "common/main.h"
#  include "master/changelog.h"
#  include "master/datacachemgr.h"
#  include "master/matoclserv.h"
#  include "master/matocsserv.h"
#endif

#define USE_FREENODE_BUCKETS 1
#define USE_CUIDREC_BUCKETS 1
#define EDGEHASH 1

#define NODEHASHBITS (22)
#define NODEHASHSIZE (1<<NODEHASHBITS)
#define NODEHASHPOS(nodeid) ((nodeid)&(NODEHASHSIZE-1))

#ifdef EDGEHASH
#define EDGEHASHBITS (22)
#define EDGEHASHSIZE (1<<EDGEHASHBITS)
#define EDGEHASHPOS(hash) ((hash)&(EDGEHASHSIZE-1))
#define LOOKUPNOHASHLIMIT 10
#endif

#define XATTR_INODE_HASH_SIZE 65536
#define XATTR_DATA_HASH_SIZE 524288

/*
#ifdef CACHENOTIFY
#define ATTR_CACHE_DISABLE_DELTA 10
#define ATTR_CACHE_ENABLE_DELTA 120
#endif
*/

//#define GOAL(x) ((x)&0xF)
//#define DELETE(x) (((x)>>4)&1)
//#define SETGOAL(x,y) ((x)=((x)&0xF0)|((y)&0xF))
//#define SETDELETE(x,y) ((x)=((x)&0xF)|((y)&0x10))
#define DEFAULT_GOAL 1
#define DEFAULT_TRASHTIME 86400

#define MAXFNAMELENG 255

#define MAX_INDEX 0x7FFFFFFF

#define CHIDS_NO 0
#define CHIDS_YES 1
#define CHIDS_AUTO 2

enum class AclInheritance {
	kInheritAcl,
	kDontInheritAcl,
};

constexpr uint8_t kMetadataVersionMooseFS  = 0x15;
constexpr uint8_t kMetadataVersionLizardFS = 0x16;
constexpr uint8_t kMetadataVersionWithSections = 0x20;

#ifndef METARESTORE
typedef struct _bstnode {
	uint32_t val,count;
	struct _bstnode *left,*right;
} bstnode;
#endif

typedef struct _sessionidrec {
	uint32_t sessionid;
	struct _sessionidrec *next;
} sessionidrec;

class fsnode;

typedef struct _fsedge {
	fsnode *child,*parent;
	struct _fsedge *nextchild,*nextparent;
	struct _fsedge **prevchild,**prevparent;
#ifdef EDGEHASH
	struct _fsedge *next,**prev;
#endif
	uint64_t checksum;
	uint16_t nleng;
//      uint16_t nhash;
	uint8_t *name;
} fsedge;

#ifndef METARESTORE
typedef struct _statsrecord {
	uint32_t inodes;
	uint32_t dirs;
	uint32_t files;
	uint32_t chunks;
	uint64_t length;
	uint64_t size;
	uint64_t realsize;
} statsrecord;
#endif

struct xattr_data_entry {
	uint32_t inode;
	uint8_t anleng;
	uint32_t avleng;
	uint8_t *attrname;
	uint8_t *attrvalue;
	uint64_t checksum;
	struct xattr_data_entry **previnode,*nextinode;
	struct xattr_data_entry **prev,*next;
};

struct xattr_inode_entry {
	uint32_t inode;
	uint32_t anleng;
	uint32_t avleng;
	struct xattr_data_entry *data_head;
	struct xattr_inode_entry *next;
};

static xattr_inode_entry **xattr_inode_hash;
static xattr_data_entry **xattr_data_hash;

class fsnode {
public:
	std::unique_ptr<ExtendedAcl> extendedAcl;
	std::unique_ptr<AccessControlList> defaultAcl;
	uint32_t id;
	uint32_t ctime,mtime,atime;
	uint8_t type;
	uint8_t goal;
	uint16_t mode;  // only 12 lowest bits are used for mode, in unix standard upper 4 are used for object type, but since there is field "type" this bits can be used as extra flags
	uint32_t uid;
	uint32_t gid;
	uint32_t trashtime;
	union _data {
		struct _ddata {                         // type==TYPE_DIRECTORY
			fsedge *children;
			uint32_t nlink;
			uint32_t elements;
#ifndef METARESTORE
			statsrecord *stats;
#endif
		} ddata;
		struct _sdata {                         // type==TYPE_SYMLINK
			uint32_t pleng;
			uint8_t *path;
/*
#ifdef CACHENOTIFY
			uint32_t lastattrchange;        // even - store attr in cache / odd - do not store attr in cache
#endif
*/
		} sdata;
		struct _devdata {
			uint32_t rdev;                          // type==TYPE_BLOCKDEV ; type==TYPE_CHARDEV
/*
#ifdef CACHENOTIFY
			uint32_t lastattrchange;        // even - store attr in cache / odd - do not store attr in cache
#endif
*/
		} devdata;
		struct _fdata {                         // type==TYPE_FILE ; type==TYPE_TRASH ; type==TYPE_RESERVED
			uint64_t length;
			uint64_t *chunktab;
			uint32_t chunks;
/*
#ifdef CACHENOTIFY
			uint32_t lastattrchange;        // even - store attr in cache / odd - do not store attr in cache
#endif
*/
			sessionidrec *sessionids;
		} fdata;
/*
#ifdef CACHENOTIFY
		struct _odata {
			uint32_t lastattrchange;        // even - store attr in cache / odd - do not store attr in cache
		} odata;
#endif
*/
	} data;
	fsedge *parents;
	fsnode *next;
	uint64_t checksum;
};

typedef struct _freenode {
	uint32_t id;
	uint32_t ftime;
	struct _freenode *next;
} freenode;

static uint32_t *freebitmask;
static uint32_t bitmasksize;
static uint32_t searchpos;
static freenode *freelist,**freetail;

static fsedge *trash;
static fsedge *reserved;
static fsnode *root;
static fsnode* nodehash[NODEHASHSIZE];
#ifdef EDGEHASH
static fsedge* edgehash[EDGEHASHSIZE];
#endif

static uint32_t maxnodeid;
static uint32_t nextsessionid;
static uint32_t nodes;

static uint64_t metaversion;
static uint64_t trashspace;
static uint64_t reservedspace;
static uint32_t trashnodes;
static uint32_t reservednodes;
static uint32_t filenodes;
static uint32_t dirnodes;

static QuotaDatabase gQuotaDatabase;

#ifndef METARESTORE

static uint32_t gStoredPreviousBackMetaCopies;
const uint32_t kDefaultStoredPreviousBackMetaCopies = 1;
const uint32_t kMaxStoredPreviousBackMetaCopies = 99;

#define MSGBUFFSIZE 1000000
#define ERRORS_LOG_MAX 500

static uint32_t fsinfo_files=0;
static uint32_t fsinfo_ugfiles=0;
static uint32_t fsinfo_mfiles=0;
static uint32_t fsinfo_chunks=0;
static uint32_t fsinfo_ugchunks=0;
static uint32_t fsinfo_mchunks=0;
static char *fsinfo_msgbuff=NULL;
static uint32_t fsinfo_msgbuffleng=0;
static uint32_t fsinfo_loopstart=0;
static uint32_t fsinfo_loopend=0;

static uint32_t test_start_time;

static uint32_t stats_statfs=0;
static uint32_t stats_getattr=0;
static uint32_t stats_setattr=0;
static uint32_t stats_lookup=0;
static uint32_t stats_mkdir=0;
static uint32_t stats_rmdir=0;
static uint32_t stats_symlink=0;
static uint32_t stats_readlink=0;
static uint32_t stats_mknod=0;
static uint32_t stats_unlink=0;
static uint32_t stats_rename=0;
static uint32_t stats_link=0;
static uint32_t stats_readdir=0;
static uint32_t stats_open=0;
static uint32_t stats_read=0;
static uint32_t stats_write=0;

void fs_stats(uint32_t stats[16]) {
	stats[0] = stats_statfs;
	stats[1] = stats_getattr;
	stats[2] = stats_setattr;
	stats[3] = stats_lookup;
	stats[4] = stats_mkdir;
	stats[5] = stats_rmdir;
	stats[6] = stats_symlink;
	stats[7] = stats_readlink;
	stats[8] = stats_mknod;
	stats[9] = stats_unlink;
	stats[10] = stats_rename;
	stats[11] = stats_link;
	stats[12] = stats_readdir;
	stats[13] = stats_open;
	stats[14] = stats_read;
	stats[15] = stats_write;
	stats_statfs=0;
	stats_getattr=0;
	stats_setattr=0;
	stats_lookup=0;
	stats_mkdir=0;
	stats_rmdir=0;
	stats_symlink=0;
	stats_readlink=0;
	stats_mknod=0;
	stats_unlink=0;
	stats_rename=0;
	stats_link=0;
	stats_readdir=0;
	stats_open=0;
	stats_read=0;
	stats_write=0;
}

static void renameBackupFile(bool ifExistsOnly, const std::string& from, const std::string& to) {
	if (ifExistsOnly) {
		if (access(from.c_str(), F_OK) != 0 && errno == ENOENT) {
			return;
		}
	}
	if (rename(from.c_str(), to.c_str()) != 0) {
		mfs_arg_errlog(LOG_ERR, "rename backup file %s to %s failed", from.c_str(), to.c_str());
	}
}

static void renameBackupFiles() {
	// rename previous backups
	if (gStoredPreviousBackMetaCopies > 0) {
		std::string from, to;
		for (int n = gStoredPreviousBackMetaCopies; n > 1; n--) {
			renameBackupFile(true,
					METADATA_BACK_FILENAME "." + std::to_string(n - 1),
					METADATA_BACK_FILENAME "." + std::to_string(n));
		}
		renameBackupFile(true, METADATA_BACK_FILENAME, METADATA_BACK_FILENAME ".1");
	}
	renameBackupFile(false, METADATA_BACK_TMP_FILENAME, METADATA_BACK_FILENAME);
	unlink(METADATA_FILENAME);
}

#endif // ifndef METARESTORE

static uint64_t gFsNodesChecksum;

static uint64_t fsnodes_checksum(const fsnode* node) {
	if (!node) {
		return 0;
	}
	uint64_t seed = 0x4660fe60565ba616; // random number
	hashCombine(seed, node->type, node->id, node->goal, node->mode, node->uid, node->gid,
			node->atime, node->mtime, node->ctime, node->trashtime);
	switch (node->type) {
		case TYPE_DIRECTORY:
		case TYPE_SOCKET:
		case TYPE_FIFO:
			break;
		case TYPE_BLOCKDEV:
		case TYPE_CHARDEV:
			hashCombine(seed, node->data.devdata.rdev);
			break;
		case TYPE_SYMLINK:
			hashCombine(seed, node->data.sdata.pleng, node->data.sdata.path,
					node->data.sdata.pleng);
			break;
		case TYPE_FILE:
		case TYPE_TRASH:
		case TYPE_RESERVED:
			hashCombine(seed, node->data.fdata.length);
			// first chunk's id
			if (node->data.fdata.length == 0 || node->data.fdata.chunks == 0) {
				hashCombine(seed, static_cast<uint64_t>(0));
			} else {
				hashCombine(seed, node->data.fdata.chunktab[0]);
			}
			// last chunk's id
			uint32_t lastchunk = (node->data.fdata.length - 1) / MFSCHUNKSIZE;
			if (node->data.fdata.length == 0 || lastchunk >= node->data.fdata.chunks) {
				hashCombine(seed, static_cast<uint64_t>(0));
			} else {
				hashCombine(seed, node->data.fdata.chunktab[lastchunk]);
			}
	}
	return seed;
}

static void fsnodes_update_checksum(fsnode* node) {
	if (!node) {
		return;
	}
	removeFromChecksum(gFsNodesChecksum, node->checksum);
	node->checksum = fsnodes_checksum(node);
	addToChecksum(gFsNodesChecksum, node->checksum);
}

static void fsnodes_recalculate_checksum() {
	gFsNodesChecksum = 12345; // arbitrary number
	// nodes
	for (uint32_t i = 0; i < NODEHASHSIZE; i++) {
		for (fsnode* node = nodehash[i]; node; node = node->next) {
			node->checksum = fsnodes_checksum(node);
			addToChecksum(gFsNodesChecksum, node->checksum);
		}
	}
}

static uint64_t gFsEdgesChecksum;

static uint64_t fsedges_checksum(const fsedge* edge) {
	if (!edge) {
		return 0;
	}
	uint64_t seed = 0xb14f9f1819ff266c; // random number
	if (edge->parent) {
		hashCombine(seed, edge->parent->id);
	}
	hashCombine(seed, edge->child->id, edge->nleng, edge->name, edge->nleng);
	return seed;
}

static void fsedges_checksum_edges_list(uint64_t& checksum, fsedge* edge) {
	while (edge) {
		edge->checksum = fsedges_checksum(edge);
		addToChecksum(checksum, edge->checksum);
		edge = edge->nextchild;
	}
}

static void fsedges_checksum_edges_rec(uint64_t& checksum, fsnode* node) {
	if (!node) {
		return;
	}
	fsedges_checksum_edges_list(checksum, node->data.ddata.children);
	for (const fsedge* edge = node->data.ddata.children; edge; edge = edge->nextchild) {
		if (edge->child->type == TYPE_DIRECTORY) {
			fsedges_checksum_edges_rec(checksum, edge->child);
		}
	}
}

static void fsedges_update_checksum(fsedge* edge) {
	if (!edge) {
		return;
	}
	removeFromChecksum(gFsEdgesChecksum, edge->checksum);
	edge->checksum = fsedges_checksum(edge);
	addToChecksum(gFsEdgesChecksum, edge->checksum);
}

static void fsedges_recalculate_checksum() {
	gFsEdgesChecksum = 1231241261;
	// edges
	if (root) {
		fsedges_checksum_edges_rec(gFsEdgesChecksum, root);
	}
	if (trash) {
		fsedges_checksum_edges_list(gFsEdgesChecksum, trash);
	}
	if (reserved) {
		fsedges_checksum_edges_list(gFsEdgesChecksum, reserved);
	}
}

static uint64_t gXattrChecksum;

static uint64_t xattr_checksum(const xattr_data_entry* xde) {
	if (!xde) {
		return 0;
	}
	uint64_t seed = 645819511511147ULL;
	hashCombine(seed, xde->inode, xde->attrname, xde->anleng, xde->attrvalue, xde->avleng);
	return seed;
}

static void xattr_update_checksum(xattr_data_entry* xde) {
	if (!xde) {
		return;
	}
	removeFromChecksum(gXattrChecksum, xde->checksum);
	xde->checksum = xattr_checksum(xde);
	addToChecksum(gXattrChecksum, xde->checksum);
}

static void xattr_recalculate_checksum() {
	gXattrChecksum = 29857986791741783ULL;
	for (int i = 0; i < XATTR_DATA_HASH_SIZE; ++i) {
		for (xattr_data_entry* xde = xattr_data_hash[i]; xde; xde = xde->next) {
			xde->checksum = xattr_checksum(xde);
			addToChecksum(gXattrChecksum, xde->checksum);
		}
	}
}

uint64_t fs_checksum(ChecksumMode mode) {
	uint64_t checksum = 0x1251;
	addToChecksum(checksum, maxnodeid);
	addToChecksum(checksum, metaversion);
	addToChecksum(checksum, nextsessionid);
	if (mode == ChecksumMode::kForceRecalculate) {
		fsnodes_recalculate_checksum();
		fsedges_recalculate_checksum();
		xattr_recalculate_checksum();
	}
	addToChecksum(checksum, gFsNodesChecksum);
	addToChecksum(checksum, gFsEdgesChecksum);
	addToChecksum(checksum, gXattrChecksum);
	addToChecksum(checksum, gQuotaDatabase.checksum());
	addToChecksum(checksum, chunk_checksum(mode));
	return checksum;
}

static MetadataDumper metadataDumper(METADATA_BACK_FILENAME, METADATA_BACK_TMP_FILENAME);

#ifndef METARESTORE
void metadataPollDesc(struct pollfd* pdesc, uint32_t* ndesc) {
	metadataDumper.pollDesc(pdesc, ndesc);
}
void metadataPollServe(struct pollfd* pdesc) {
	bool metadataDumpInProgress = metadataDumper.inProgress();
	metadataDumper.pollServe(pdesc);
	if (metadataDumpInProgress && !metadataDumper.inProgress()) {
		if (metadataDumper.dumpSucceeded()) {
			renameBackupFiles();
		} else {
			if (metadataDumper.useMetarestore()) {
				// master should recalculate its checksum
				syslog(LOG_WARNING, "dumping metadata failed, recalculating checksum");
				fs_checksum(ChecksumMode::kForceRecalculate);
			}
			unlink(METADATA_BACK_TMP_FILENAME);
		}
	}
}
#endif

#ifdef USE_FREENODE_BUCKETS
#define FREENODE_BUCKET_SIZE 5000

typedef struct _freenode_bucket {
	freenode bucket[FREENODE_BUCKET_SIZE];
	uint32_t firstfree;
	struct _freenode_bucket *next;
} freenode_bucket;

static freenode_bucket *fnbhead = NULL;
static freenode *fnfreehead = NULL;

static inline freenode* freenode_malloc() {
	freenode_bucket *fnb;
	freenode *ret;
	if (fnfreehead) {
		ret = fnfreehead;
		fnfreehead = ret->next;
		return ret;
	}
	if (fnbhead==NULL || fnbhead->firstfree==FREENODE_BUCKET_SIZE) {
		fnb = (freenode_bucket*)malloc(sizeof(freenode_bucket));
		passert(fnb);
		fnb->next = fnbhead;
		fnb->firstfree = 0;
		fnbhead = fnb;
	}
	ret = (fnbhead->bucket)+(fnbhead->firstfree);
	fnbhead->firstfree++;
	return ret;
}

static inline void freenode_free(freenode *p) {
	p->next = fnfreehead;
	fnfreehead = p;
}
#else /* USE_FREENODE_BUCKETS */

static inline freenode* freenode_malloc() {
	freenode *fn;
	fn = (freenode*)malloc(sizeof(freenode));
	passert(fn);
	return fn;
}

static inline void freenode_free(freenode* p) {
	free(p);
}

#endif /* USE_FREENODE_BUCKETS */

#ifdef USE_CUIDREC_BUCKETS
#define CUIDREC_BUCKET_SIZE 1000

typedef struct _sessionidrec_bucket {
	sessionidrec bucket[CUIDREC_BUCKET_SIZE];
	uint32_t firstfree;
	struct _sessionidrec_bucket *next;
} sessionidrec_bucket;

static sessionidrec_bucket *crbhead = NULL;
static sessionidrec *crfreehead = NULL;

static inline sessionidrec* sessionidrec_malloc() {
	sessionidrec_bucket *crb;
	sessionidrec *ret;
	if (crfreehead) {
		ret = crfreehead;
		crfreehead = ret->next;
		return ret;
	}
	if (crbhead==NULL || crbhead->firstfree==CUIDREC_BUCKET_SIZE) {
		crb = (sessionidrec_bucket*)malloc(sizeof(sessionidrec_bucket));
		passert(crb);
		crb->next = crbhead;
		crb->firstfree = 0;
		crbhead = crb;
	}
	ret = (crbhead->bucket)+(crbhead->firstfree);
	crbhead->firstfree++;
	return ret;
}

static inline void sessionidrec_free(sessionidrec *p) {
	p->next = crfreehead;
	crfreehead = p;
}
#else /* USE_CUIDREC_BUCKETS */

static inline sessionidrec* sessionidrec_malloc() {
	sessionidrec *sidrec;
	sidrec = (sessionidrec*)malloc(sizeof(sessionidrec));
	passert(sidrec);
	return sidrec;
}

static inline void sessionidrec_free(sessionidrec* p) {
	free(p);
}

#endif /* USE_CUIDREC_BUCKETS */

uint32_t fsnodes_get_next_id() {
	uint32_t i,mask;
	while (searchpos<bitmasksize && freebitmask[searchpos]==0xFFFFFFFF) {
		searchpos++;
	}
	if (searchpos==bitmasksize) {   // no more freeinodes
		uint32_t *tmpfbm;
		bitmasksize+=0x80;
		tmpfbm = freebitmask;
		freebitmask = (uint32_t*)realloc(freebitmask,bitmasksize*sizeof(uint32_t));
		if (freebitmask==NULL) {
			free(tmpfbm); // pro forma - satisfy cppcheck
		}
		passert(freebitmask);
		memset(freebitmask+searchpos,0,0x80*sizeof(uint32_t));
	}
	mask = freebitmask[searchpos];
	i=0;
	while (mask&1) {
		i++;
		mask>>=1;
	}
	mask = 1<<i;
	freebitmask[searchpos] |= mask;
	i+=(searchpos<<5);
	if (i>maxnodeid) {
		maxnodeid=i;
	}
	return i;
}

void fsnodes_free_id(uint32_t id,uint32_t ts) {
	freenode *n;
	n = freenode_malloc();
	n->id = id;
	n->ftime = ts;
	n->next = NULL;
	*freetail = n;
	freetail = &(n->next);
}

#ifndef METARESTORE
void fsnodes_freeinodes(void) {
#else
uint8_t fs_freeinodes(uint32_t ts,uint32_t freeinodes) {
#endif
	uint32_t fi,now,pos,mask;
	freenode *n,*an;
#ifndef METARESTORE
	now = main_time();
#else
	now = ts;
#endif
	fi = 0;
	n = freelist;
	while (n && n->ftime+86400<now) {
		fi++;
		pos = (n->id >> 5);
		mask = 1<<(n->id&0x1F);
		freebitmask[pos] &= ~mask;
		if (pos<searchpos) {
			searchpos = pos;
		}
		an = n->next;
		freenode_free(n);
		n = an;
	}
	if (n) {
		freelist = n;
	} else {
		freelist = NULL;
		freetail = &(freelist);
	}
#ifndef METARESTORE
	if (fi>0) {
		changelog(metaversion++,"%" PRIu32 "|FREEINODES():%" PRIu32,(uint32_t)main_time(),fi);
	}
#else
	metaversion++;
	if (freeinodes!=fi) {
		return 1;
	}
	return 0;
#endif
}

void fsnodes_init_freebitmask (void) {
	bitmasksize = 0x100+(((maxnodeid)>>5)&0xFFFFFF80U);
	freebitmask = (uint32_t*)malloc(bitmasksize*sizeof(uint32_t));
	passert(freebitmask);
	memset(freebitmask,0,bitmasksize*sizeof(uint32_t));
	freebitmask[0]=1;       // reserve inode 0
	searchpos = 0;
}

void fsnodes_used_inode (uint32_t id) {
	uint32_t pos,mask;
	pos = id>>5;
	mask = 1<<(id&0x1F);
	freebitmask[pos]|=mask;
}


/* xattr */

static inline uint32_t xattr_inode_hash_fn(uint32_t inode) {
	return ((inode*0x72B5F387U)&(XATTR_INODE_HASH_SIZE-1));
}

static inline uint32_t xattr_data_hash_fn(uint32_t inode,uint8_t anleng,const uint8_t *attrname) {
	uint32_t hash = inode*5381U;
	while (anleng) {
		hash = (hash * 33U) + (*attrname);
		attrname++;
		anleng--;
	}
	return (hash&(XATTR_DATA_HASH_SIZE-1));
}

static inline int xattr_namecheck(uint8_t anleng,const uint8_t *attrname) {
	uint32_t i;
	for (i=0 ; i<anleng ; i++) {
		if (attrname[i]=='\0') {
			return -1;
		}
	}
	return 0;
}

static inline void xattr_removeentry(xattr_data_entry *xa) {
	*(xa->previnode) = xa->nextinode;
	if (xa->nextinode) {
		xa->nextinode->previnode = xa->previnode;
	}
	*(xa->prev) = xa->next;
	if (xa->next) {
		xa->next->prev = xa->prev;
	}
	free(xa->attrname);
	if (xa->attrvalue) {
		free(xa->attrvalue);
	}
	removeFromChecksum(gXattrChecksum, xa->checksum);
	free(xa);
}

void xattr_removeinode(uint32_t inode) {
	xattr_inode_entry *ih,**ihp;

	ihp = &(xattr_inode_hash[xattr_inode_hash_fn(inode)]);
	while ((ih = *ihp)) {
		if (ih->inode==inode) {
			while (ih->data_head) {
				xattr_removeentry(ih->data_head);
			}
			*ihp = ih->next;
			free(ih);
		} else {
			ihp = &(ih->next);
		}
	}
}

uint8_t xattr_setattr(uint32_t inode,uint8_t anleng,const uint8_t *attrname,uint32_t avleng,const uint8_t *attrvalue,uint8_t mode) {
	xattr_inode_entry *ih;
	xattr_data_entry *xa;
	uint32_t hash,ihash;

	if (avleng>MFS_XATTR_SIZE_MAX) {
		return ERROR_ERANGE;
	}
#if MFS_XATTR_NAME_MAX<255
	if (anleng==0U || anleng>MFS_XATTR_NAME_MAX) {
#else
	if (anleng==0U) {
#endif
		return ERROR_EINVAL;
	}

	ihash = xattr_inode_hash_fn(inode);
	for (ih = xattr_inode_hash[ihash]; ih && ih->inode!=inode; ih=ih->next) {}

	hash = xattr_data_hash_fn(inode,anleng,attrname);
	for (xa = xattr_data_hash[hash]; xa ; xa=xa->next) {
		if (xa->inode==inode && xa->anleng==anleng && memcmp(xa->attrname,attrname,anleng)==0) {
			passert(ih);
			if (mode==MFS_XATTR_CREATE_ONLY) { // create only
				return ERROR_EEXIST;
			}
			if (mode==MFS_XATTR_REMOVE) { // remove
				ih->anleng -= anleng+1U;
				ih->avleng -= xa->avleng;
				xattr_removeentry(xa);
				if (ih->data_head==NULL) {
					if (ih->anleng!=0 || ih->avleng!=0) {
						syslog(LOG_WARNING,"xattr non zero lengths on remove (inode:%" PRIu32 ",anleng:%" PRIu32 ",avleng:%" PRIu32 ")",ih->inode,ih->anleng,ih->avleng);
					}
					xattr_removeinode(inode);
				}
				return STATUS_OK;
			}
			ih->avleng -= xa->avleng;
			if (xa->attrvalue) {
				free(xa->attrvalue);
			}
			if (avleng>0) {
				xa->attrvalue = (uint8_t*) malloc(avleng);
				passert(xa->attrvalue);
				memcpy(xa->attrvalue,attrvalue,avleng);
			} else {
				xa->attrvalue = NULL;
			}
			xa->avleng = avleng;
			ih->avleng += avleng;
			xattr_update_checksum(xa);
			return STATUS_OK;
		}
	}

	if (mode==MFS_XATTR_REPLACE_ONLY || mode==MFS_XATTR_REMOVE) {
		return ERROR_ENOATTR;
	}

	if (ih && ih->anleng+anleng+1>MFS_XATTR_LIST_MAX) {
		return ERROR_ERANGE;
	}

	xa = (xattr_data_entry*) malloc(sizeof(xattr_data_entry));
	passert(xa);
	xa->inode = inode;
	xa->attrname = (uint8_t*) malloc(anleng);
	passert(xa->attrname);
	memcpy(xa->attrname,attrname,anleng);
	xa->anleng = anleng;
	if (avleng>0) {
		xa->attrvalue = (uint8_t*) malloc(avleng);
		passert(xa->attrvalue);
		memcpy(xa->attrvalue,attrvalue,avleng);
	} else {
		xa->attrvalue = NULL;
	}
	xa->avleng = avleng;
	xa->next = xattr_data_hash[hash];
	if (xa->next) {
		xa->next->prev = &(xa->next);
	}
	xa->prev = xattr_data_hash + hash;
	xattr_data_hash[hash] = xa;

	if (ih) {
		xa->nextinode = ih->data_head;
		if (xa->nextinode) {
			xa->nextinode->previnode = &(xa->nextinode);
		}
		xa->previnode = &(ih->data_head);
		ih->data_head = xa;
		ih->anleng += anleng+1U;
		ih->avleng += avleng;
	} else {
		ih = (xattr_inode_entry*) malloc(sizeof(xattr_inode_entry));
		passert(ih);
		ih->inode = inode;
		xa->nextinode = NULL;
		xa->previnode = &(ih->data_head);
		ih->data_head = xa;
		ih->anleng = anleng+1U;
		ih->avleng = avleng;
		ih->next = xattr_inode_hash[ihash];
		xattr_inode_hash[ihash] = ih;
	}
	xa->checksum = 0;
	xattr_update_checksum(xa);
	return STATUS_OK;
}

uint8_t xattr_getattr(uint32_t inode,uint8_t anleng,const uint8_t *attrname,uint32_t *avleng,uint8_t **attrvalue) {
	xattr_data_entry *xa;

	for (xa = xattr_data_hash[xattr_data_hash_fn(inode,anleng,attrname)] ; xa ; xa=xa->next) {
		if (xa->inode==inode && xa->anleng==anleng && memcmp(xa->attrname,attrname,anleng)==0) {
			if (xa->avleng>MFS_XATTR_SIZE_MAX) {
				return ERROR_ERANGE;
			}
			*attrvalue = xa->attrvalue;
			*avleng = xa->avleng;
			return STATUS_OK;
		}
	}
	return ERROR_ENOATTR;
}

uint8_t xattr_listattr_leng(uint32_t inode,void **xanode,uint32_t *xasize) {
	xattr_inode_entry *ih;
	xattr_data_entry *xa;

	*xasize = 0;
	for (ih = xattr_inode_hash[xattr_inode_hash_fn(inode)] ; ih ; ih=ih->next) {
		if (ih->inode==inode) {
			*xanode = ih;
			for (xa=ih->data_head ; xa ; xa=xa->nextinode) {
				*xasize += xa->anleng+1U;
			}
			if (*xasize>MFS_XATTR_LIST_MAX) {
				return ERROR_ERANGE;
			}
			return STATUS_OK;
		}
	}
	*xanode = NULL;
	return STATUS_OK;
}

void xattr_listattr_data(void *xanode,uint8_t *xabuff) {
	xattr_inode_entry *ih = (xattr_inode_entry*)xanode;
	xattr_data_entry *xa;
	uint32_t l;

	l = 0;
	if (ih) {
		for (xa=ih->data_head ; xa ; xa=xa->nextinode) {
			memcpy(xabuff+l,xa->attrname,xa->anleng);
			l+=xa->anleng;
			xabuff[l++]=0;
		}
	}
}

static char* fsnodes_escape_name(uint32_t nleng,const uint8_t *name) {
	static char *escname[2]={NULL,NULL};
	static uint32_t escnamesize[2]={0,0};
	static uint8_t buffid=0;
	char *currescname=NULL;
	uint32_t i;
	uint8_t c;
	buffid = 1-buffid;
	i = nleng;
	i = i*3+1;
	if (i>escnamesize[buffid] || i==0) {
		escnamesize[buffid] = ((i/1000)+1)*1000;
		if (escname[buffid]!=NULL) {
			free(escname[buffid]);
		}
		escname[buffid] = (char*) malloc(escnamesize[buffid]);
		passert(escname[buffid]);
	}
	i = 0;
	currescname = escname[buffid];
	passert(currescname);
	while (nleng>0) {
		c = *name;
		if (c<32 || c>=127 || c==',' || c=='%' || c=='(' || c==')') {
			currescname[i++]='%';
			currescname[i++]="0123456789ABCDEF"[(c>>4)&0xF];
			currescname[i++]="0123456789ABCDEF"[c&0xF];
		} else {
			currescname[i++]=c;
		}
		name++;
		nleng--;
	}
	currescname[i]=0;
	return currescname;
}

#ifdef EDGEHASH
static inline uint32_t fsnodes_hash(uint32_t parentid,uint16_t nleng,const uint8_t *name) {
	uint32_t hash,i;
	hash = ((parentid * 0x5F2318BD) + nleng);
	for (i=0 ; i<nleng ; i++) {
		hash = hash*33+name[i];
	}
	return hash;
}
#endif

static inline int fsnodes_nameisused(fsnode *node,uint16_t nleng,const uint8_t *name) {
	fsedge *ei;
#ifdef EDGEHASH
	if (node->data.ddata.elements>LOOKUPNOHASHLIMIT) {
		ei = edgehash[EDGEHASHPOS(fsnodes_hash(node->id,nleng,name))];
		while (ei) {
			if (ei->parent==node && nleng==ei->nleng && memcmp((char*)(ei->name),(char*)name,nleng)==0) {
				return 1;
			}
			ei = ei->next;
		}
	} else {
		ei = node->data.ddata.children;
		while (ei) {
			if (nleng==ei->nleng && memcmp((char*)(ei->name),(char*)name,nleng)==0) {
				return 1;
			}
			ei = ei->nextchild;
		}
	}
#else
	ei = node->data.ddata.children;
	while (ei) {
		if (nleng==ei->nleng && memcmp((char*)(ei->name),(char*)name,nleng)==0) {
			return 1;
		}
		ei = ei->nextchild;
	}
#endif
	return 0;
}

/// searches for an edge with given name (`name`) in given directory (`node`)
static inline fsedge* fsnodes_lookup(fsnode *node,uint16_t nleng,const uint8_t *name) {
	fsedge *ei;

	if (node->type!=TYPE_DIRECTORY) {
		return NULL;
	}
#ifdef EDGEHASH
	if (node->data.ddata.elements>LOOKUPNOHASHLIMIT) {
		ei = edgehash[EDGEHASHPOS(fsnodes_hash(node->id,nleng,name))];
		while (ei) {
			if (ei->parent==node && nleng==ei->nleng && memcmp((char*)(ei->name),(char*)name,nleng)==0) {
				return ei;
			}
			ei = ei->next;
		}
	} else {
		ei = node->data.ddata.children;
		while (ei) {
			if (nleng==ei->nleng && memcmp((char*)(ei->name),(char*)name,nleng)==0) {
				return ei;
			}
			ei = ei->nextchild;
		}
	}
#else
	ei = node->data.ddata.children;
	while (ei) {
		if (nleng==ei->nleng && memcmp((char*)(ei->name),(char*)name,nleng)==0) {
			return ei;
		}
		ei = ei->nextchild;
	}
#endif
	return NULL;
}

static inline fsnode* fsnodes_id_to_node(uint32_t id) {
	fsnode *p;
	uint32_t nodepos = NODEHASHPOS(id);
	for (p=nodehash[nodepos]; p ; p=p->next ) {
		if (p->id == id) {
			return p;
		}
	}
	return NULL;
}

// returns 1 only if f is ancestor of p
static inline int fsnodes_isancestor(fsnode *f,fsnode *p) {
	fsedge *e;
	for (e=p->parents ; e ; e=e->nextparent) {      // check all parents of 'p' because 'p' can be any object, so it can be hardlinked
		p=e->parent;    // warning !!! since this point 'p' is used as temporary variable
		while (p) {
			if (f==p) {
				return 1;
			}
			if (p->parents) {
				p = p->parents->parent; // here 'p' is always a directory so it should have only one parent
			} else {
				p = NULL;
			}
		}
	}
	return 0;
}

// quota

#ifndef METARESTORE
static bool fsnodes_inode_quota_exceeded(uint32_t uid, uint32_t gid) {
	return gQuotaDatabase.isExceeded(QuotaRigor::kHard, QuotaResource::kInodes, uid, gid);
}

static bool fsnodes_size_quota_exceeded(uint32_t uid, uint32_t gid) {
	return gQuotaDatabase.isExceeded(QuotaRigor::kHard, QuotaResource::kSize, uid, gid);
}

static void fsnodes_quota_register_inode(fsnode *node) {
	gQuotaDatabase.changeUsage(QuotaResource::kInodes, node->uid, node->gid, +1);
}

static void fsnodes_quota_unregister_inode(fsnode *node) {
	gQuotaDatabase.changeUsage(QuotaResource::kInodes, node->uid, node->gid, -1);
}

static void fsnodes_quota_update_size(fsnode *node, int64_t delta) {
	if (delta != 0) {
		gQuotaDatabase.changeUsage(QuotaResource::kSize, node->uid, node->gid, delta);
	}
}
#endif

// stats
#ifndef METARESTORE

// does the last chunk exist and contain non-zero data?
static bool last_chunk_nonempty(fsnode *node) {
	const uint32_t chunks = node->data.fdata.chunks;
	if (chunks == 0) {
		// no non-zero chunks, return now
		return false;
	}
	// file has non-zero length and contains at least one chunk
	const uint64_t last_byte = node->data.fdata.length - 1;
	const uint32_t last_chunk = last_byte / MFSCHUNKSIZE;
	if (last_chunk < chunks) {
		// last chunk exists, check if it isn't the zero chunk
		return (node->data.fdata.chunktab[last_chunk] != 0);
	} else {
		// last chunk hasn't been allocated yet
		return false;
	}
}

// number of blocks in the last chunk before EOF
static uint32_t last_chunk_blocks(fsnode *node) {
	const uint64_t last_byte = node->data.fdata.length - 1;
	const uint32_t last_byte_offset = last_byte % MFSCHUNKSIZE;
	const uint32_t last_block = last_byte_offset / MFSBLOCKSIZE;
	const uint32_t block_count = last_block + 1;
	return block_count;
}

// count chunks in a file, disregard sparse file holes
static uint32_t file_chunks(fsnode *node) {
	uint32_t count = 0;
	for (uint64_t i = 0; i < node->data.fdata.chunks; i++) {
		if (node->data.fdata.chunktab[i] != 0) {
			count++;
		}
	}
	return count;
}

// compute the "size" statistic for a file node
static uint64_t file_size(fsnode *node, uint32_t nonzero_chunks) {
	uint64_t size = (uint64_t)nonzero_chunks * (MFSCHUNKSIZE + MFSHDRSIZE);
	if (last_chunk_nonempty(node)) {
		size -= MFSCHUNKSIZE;
		size += last_chunk_blocks(node) * MFSBLOCKSIZE;
	}
	return size;
}

// compute the disk space cost of all parts of a xor chunk of given size
static uint32_t xor_chunk_realsize(uint32_t blocks, uint32_t level) {
	const uint32_t stripes = (blocks + level - 1) / level;
	uint32_t size = blocks * MFSBLOCKSIZE;  // file data
	size += stripes * MFSBLOCKSIZE;         // parity data
	size += 4096 * (level + 1);             // headers of data and parity parts
	return size;
}

// compute the "realsize" statistic for a file node
static uint64_t file_realsize(fsnode *node, uint32_t nonzero_chunks, uint64_t file_size) {
	const uint8_t goal = node->goal;
	if (isOrdinaryGoal(goal)) {
		return file_size * goal;
	}
	if (isXorGoal(goal)) {
		const ChunkType::XorLevel level = goalToXorLevel(goal);
		const uint32_t full_chunk_realsize = xor_chunk_realsize(MFSBLOCKSINCHUNK, level);
		uint64_t size = (uint64_t)nonzero_chunks * full_chunk_realsize;
		if (last_chunk_nonempty(node)) {
			size -= full_chunk_realsize;
			size += xor_chunk_realsize(last_chunk_blocks(node), level);
		}
		return size;
	}
	syslog(LOG_ERR, "file_realsize: inode %" PRIu32 " has unknown goal 0x%" PRIx8, node->id, node->goal);
	return 0;
}

static inline void fsnodes_get_stats(fsnode *node,statsrecord *sr) {
	switch (node->type) {
	case TYPE_DIRECTORY:
		*sr = *(node->data.ddata.stats);
		sr->inodes++;
		sr->dirs++;
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		sr->inodes = 1;
		sr->dirs = 0;
		sr->files = 1;
		sr->chunks = file_chunks(node);
		sr->length = node->data.fdata.length;
		sr->size = file_size(node, sr->chunks);
		sr->realsize = file_realsize(node, sr->chunks, sr->size);
		break;
	case TYPE_SYMLINK:
		sr->inodes = 1;
		sr->files = 0;
		sr->dirs = 0;
		sr->chunks = 0;
		sr->length = node->data.sdata.pleng;
		sr->size = 0;
		sr->realsize = 0;
		break;
	default:
		sr->inodes = 1;
		sr->files = 0;
		sr->dirs = 0;
		sr->chunks = 0;
		sr->length = 0;
		sr->size = 0;
		sr->realsize = 0;
	}
}

static int64_t fsnodes_get_size(fsnode *node) {
	statsrecord sr;
	fsnodes_get_stats(node, &sr);
	return sr.size;
}

static inline void fsnodes_sub_stats(fsnode *parent,statsrecord *sr) {
	statsrecord *psr;
	fsedge *e;
	if (parent) {
		psr = parent->data.ddata.stats;
		psr->inodes -= sr->inodes;
		psr->dirs -= sr->dirs;
		psr->files -= sr->files;
		psr->chunks -= sr->chunks;
		psr->length -= sr->length;
		psr->size -= sr->size;
		psr->realsize -= sr->realsize;
		if (parent!=root) {
			for (e=parent->parents ; e ; e=e->nextparent) {
				fsnodes_sub_stats(e->parent,sr);
			}
		}
	}
}

static inline void fsnodes_add_stats(fsnode *parent,statsrecord *sr) {
	statsrecord *psr;
	fsedge *e;
	if (parent) {
		psr = parent->data.ddata.stats;
		psr->inodes += sr->inodes;
		psr->dirs += sr->dirs;
		psr->files += sr->files;
		psr->chunks += sr->chunks;
		psr->length += sr->length;
		psr->size += sr->size;
		psr->realsize += sr->realsize;
		if (parent!=root) {
			for (e=parent->parents ; e ; e=e->nextparent) {
				fsnodes_add_stats(e->parent,sr);
			}
		}
	}
}

static inline void fsnodes_add_sub_stats(fsnode *parent,statsrecord *newsr,statsrecord *prevsr) {
	statsrecord sr;
	sr.inodes = newsr->inodes - prevsr->inodes;
	sr.dirs = newsr->dirs - prevsr->dirs;
	sr.files = newsr->files - prevsr->files;
	sr.chunks = newsr->chunks - prevsr->chunks;
	sr.length = newsr->length - prevsr->length;
	sr.size = newsr->size - prevsr->size;
	sr.realsize = newsr->realsize - prevsr->realsize;
	fsnodes_add_stats(parent,&sr);
}

#endif

#ifndef METARESTORE

static inline void fsnodes_fill_attr(fsnode *node,fsnode *parent,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t sesflags,uint8_t attr[35]) {
	uint8_t *ptr;
	uint16_t mode;
	uint32_t nlink;
/*
#ifdef CACHENOTIFY
	uint8_t attrnocache;
#endif
*/
	fsedge *e;
	(void)sesflags;
	ptr = attr;
	if (node->type==TYPE_TRASH || node->type==TYPE_RESERVED) {
		put8bit(&ptr,TYPE_FILE);
	} else {
		put8bit(&ptr,node->type);
	}
	mode = node->mode&07777;
	if (parent) {
		if (parent->mode&(EATTR_NOECACHE<<12)) {
			mode |= (MATTR_NOECACHE<<12);
		}
	}
/*
#ifdef CACHENOTIFY
	switch (node->type) {
	case TYPE_FILE:
		attrnocache = node->data.fdata.lastattrchange&1;
		break;
	case TYPE_SYMLINK:
		attrnocache = node->data.sdata.lastattrchange&1;
		break;
	case TYPE_CHARDEV:
	case TYPE_BLOCKDEV:
		attrnocache = node->data.devdata.lastattrchange&1;
		break;
	case TYPE_SOCKET:
	case TYPE_FIFO:
		attrnocache = node->data.odata.lastattrchange&1;
		break;
	default:
		attrnocache=0;
	}
	if (attrnocache || (node->mode&((EATTR_NOOWNER|EATTR_NOACACHE)<<12)) || (sesflags&SESFLAG_MAPALL)) {
//      if ((node->type==TYPE_FILE && node->data.fdata.noattrcache) || (node->mode&((EATTR_NOOWNER|EATTR_NOACACHE)<<12)) || (sesflags&SESFLAG_MAPALL)) {
#else
	... standard condition ...
#endif
*/
	if ((node->mode&((EATTR_NOOWNER|EATTR_NOACACHE)<<12)) || (sesflags&SESFLAG_MAPALL)) {
		mode |= (MATTR_NOACACHE<<12);
	}
	if ((node->mode&(EATTR_NODATACACHE<<12))==0) {
		mode |= (MATTR_ALLOWDATACACHE<<12);
	}
	put16bit(&ptr,mode);
	if ((node->mode&(EATTR_NOOWNER<<12)) && uid!=0) {
		if (sesflags&SESFLAG_MAPALL) {
			put32bit(&ptr,auid);
			put32bit(&ptr,agid);
		} else {
			put32bit(&ptr,uid);
			put32bit(&ptr,gid);
		}
	} else {
		if (sesflags&SESFLAG_MAPALL && auid!=0) {
			if (node->uid==uid) {
				put32bit(&ptr,auid);
			} else {
				put32bit(&ptr,0);
			}
			if (node->gid==gid) {
				put32bit(&ptr,agid);
			} else {
				put32bit(&ptr,0);
			}
		} else {
			put32bit(&ptr,node->uid);
			put32bit(&ptr,node->gid);
		}
	}
	put32bit(&ptr,node->atime);
	put32bit(&ptr,node->mtime);
	put32bit(&ptr,node->ctime);
	nlink = 0;
	for (e=node->parents ; e ; e=e->nextparent) {
		nlink++;
	}
	switch (node->type) {
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		put32bit(&ptr,nlink);
		put64bit(&ptr,node->data.fdata.length);
		break;
	case TYPE_DIRECTORY:
		put32bit(&ptr,node->data.ddata.nlink);
		put64bit(&ptr,node->data.ddata.stats->length>>30);      // Rescale length to GB (reduces size to 32-bit length)
		break;
	case TYPE_SYMLINK:
		put32bit(&ptr,nlink);
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		put32bit(&ptr,node->data.sdata.pleng);
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		put32bit(&ptr,nlink);
		put32bit(&ptr,node->data.devdata.rdev);
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		break;
	default:
		put32bit(&ptr,nlink);
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
	}
}

#endif /* METARESTORE */

static inline void fsnodes_remove_edge(uint32_t ts,fsedge *e) {
#ifndef METARESTORE
	statsrecord sr;
#endif
	removeFromChecksum(gFsEdgesChecksum, e->checksum);
	if (e->parent) {
#ifndef METARESTORE
		fsnodes_get_stats(e->child,&sr);
		fsnodes_sub_stats(e->parent,&sr);
#endif
		e->parent->mtime = e->parent->ctime = ts;
		e->parent->data.ddata.elements--;
		if (e->child->type==TYPE_DIRECTORY) {
			e->parent->data.ddata.nlink--;
		}
		fsnodes_update_checksum(e->parent);
	}
	if (e->child) {
		e->child->ctime = ts;
		fsnodes_update_checksum(e->child);
	}
	*(e->prevchild) = e->nextchild;
	if (e->nextchild) {
		e->nextchild->prevchild = e->prevchild;
	}
	*(e->prevparent) = e->nextparent;
	if (e->nextparent) {
		e->nextparent->prevparent = e->prevparent;
	}
#ifdef EDGEHASH
	if (e->prev) {
		*(e->prev) = e->next;
		if (e->next) {
			e->next->prev = e->prev;
		}
	}
#endif
#ifndef METARESTORE
/*
#ifdef CACHENOTIFY
	if (e->parent) {
		matoclserv_notify_unlink(e->parent->id,e->nleng,e->name,ts);
		if (e->child->type==TYPE_DIRECTORY) {
			fsnodes_attr_changed(e->parent,ts);             // nlink attr in the parent directory has changed
		} else {
			fsnodes_attr_changed_other_parents(e->child,e->parent->id,ts);  // inform other parents that nlink attr has changed
		}
	}
#endif
*/
#endif
	free(e->name);
	free(e);
}

static inline void fsnodes_link(uint32_t ts,fsnode *parent,fsnode *child,uint16_t nleng,const uint8_t *name) {
	fsedge *e;
#ifndef METARESTORE
	statsrecord sr;
/*
#ifdef CACHENOTIFY
	uint8_t attr[35];
#endif
*/
#endif
#ifdef EDGEHASH
	uint32_t hpos;
#endif

	e = (fsedge*) malloc(sizeof(fsedge));
	passert(e);
	e->nleng = nleng;
	e->name = (uint8_t*) malloc(nleng);
	passert(e->name);
	memcpy(e->name,name,nleng);
	e->child = child;
	e->parent = parent;
	e->nextchild = parent->data.ddata.children;
	if (e->nextchild) {
		e->nextchild->prevchild = &(e->nextchild);
	}
	parent->data.ddata.children = e;
	e->prevchild = &(parent->data.ddata.children);
	e->nextparent = child->parents;
	if (e->nextparent) {
		e->nextparent->prevparent = &(e->nextparent);
	}
	child->parents = e;
	e->prevparent = &(child->parents);
#ifdef EDGEHASH
	hpos = EDGEHASHPOS(fsnodes_hash(parent->id,nleng,name));
	e->next = edgehash[hpos];
	if (e->next) {
		e->next->prev = &(e->next);
	}
	edgehash[hpos] = e;
	e->prev = &(edgehash[hpos]);
#endif
	e->checksum = 0;
	fsedges_update_checksum(e);

	parent->data.ddata.elements++;
	if (child->type==TYPE_DIRECTORY) {
		parent->data.ddata.nlink++;
	}
#ifndef METARESTORE
	fsnodes_get_stats(child,&sr);
	fsnodes_add_stats(parent,&sr);
#endif
	if (ts>0) {
		parent->mtime = parent->ctime = ts;
		fsnodes_update_checksum(parent);
		child->ctime = ts;
		fsnodes_update_checksum(child);
	}
#ifndef METARESTORE
/*
#ifdef CACHENOTIFY
	if (child->type!=TYPE_DIRECTORY) {      // directory can have only one parent, so do not send this information when directory is relinked (moved)
		fsnodes_attr_changed_other_parents(child,parent->id,ts);        // inform other parents that nlink attr has changed
	}
	fsnodes_fill_attr(child,parent,0,0,0,0,0,attr);
	matoclserv_notify_link(parent->id,nleng,name,child->id,attr,ts);
	if (child->type==TYPE_DIRECTORY) {
		matoclserv_notify_parent(child->id,parent->id);
		fsnodes_attr_changed(parent,ts);                // nlink attr in the parent directory has changed
	}
#endif
*/
#endif
}

static inline fsnode* fsnodes_create_node(uint32_t ts, fsnode *node, uint16_t nleng,
		const uint8_t *name, uint8_t type, uint16_t mode, uint16_t umask, uint32_t uid,
		uint32_t gid, uint8_t copysgid, AclInheritance inheritacl) {
	fsnode *p;
#ifndef METARESTORE
	statsrecord *sr;
#endif
	uint32_t nodepos;
	p = new fsnode();
	passert(p);
	nodes++;
	if (type==TYPE_DIRECTORY) {
		dirnodes++;
	}
	if (type==TYPE_FILE) {
		filenodes++;
	}
/* create node */
	p->id = fsnodes_get_next_id();
	p->type = type;
	p->ctime = p->mtime = p->atime = ts;
	if (type==TYPE_DIRECTORY || type==TYPE_FILE) {
		p->goal = node->goal;
		p->trashtime = node->trashtime;
	} else {
		p->goal = DEFAULT_GOAL;
		p->trashtime = DEFAULT_TRASHTIME;
	}
	if (type==TYPE_DIRECTORY) {
		p->mode = (mode&07777) | (node->mode&0xF000);
	} else {
		p->mode = (mode&07777) | (node->mode&(0xF000&(~(EATTR_NOECACHE<<12))));
	}
	// If desired, node inherits permissions from parent's default ACL
	if (inheritacl == AclInheritance::kInheritAcl && node->defaultAcl) {
		if (p->type == TYPE_DIRECTORY) {
			p->defaultAcl.reset(new AccessControlList(*node->defaultAcl));
		}
		// Join ACL's access mask without cleaning sticky bits etc.
		p->mode &= ~0777 | (node->defaultAcl->mode);
		if (node->defaultAcl->extendedAcl) {
			p->extendedAcl.reset(new ExtendedAcl(*node->defaultAcl->extendedAcl));
		}
	} else {
		// Apply umask
		p->mode &= ~(umask&0777); // umask must be applied manually
	}
	p->uid = uid;
	if ((node->mode&02000)==02000) {        // set gid flag is set in the parent directory ?
		p->gid = node->gid;
		if (copysgid && type==TYPE_DIRECTORY) {
			p->mode |= 02000;
		}
	} else {
		p->gid = gid;
	}
	switch (type) {
	case TYPE_DIRECTORY:
#ifndef METARESTORE
		sr = (statsrecord*) malloc(sizeof(statsrecord));
		passert(sr);
		memset(sr,0,sizeof(statsrecord));
		p->data.ddata.stats = sr;
#endif
		p->data.ddata.children = NULL;
		p->data.ddata.nlink = 2;
		p->data.ddata.elements = 0;
		break;
	case TYPE_FILE:
		p->data.fdata.length = 0;
		p->data.fdata.chunks = 0;
		p->data.fdata.chunktab = NULL;
		p->data.fdata.sessionids = NULL;
/*
#ifdef CACHENOTIFY
		p->data.fdata.lastattrchange = 0;
#endif
*/
		break;
	case TYPE_SYMLINK:
		p->data.sdata.pleng = 0;
		p->data.sdata.path = NULL;
/*
#ifdef CACHENOTIFY
		p->data.sdata.lastattrchange = 0;
#endif
*/
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		p->data.devdata.rdev = 0;
/*
#ifdef CACHENOTIFY
		p->data.devdata.lastattrchange = 0;
		break;
	case TYPE_SOCKET:
	case TYPE_FIFO:
		p->data.odata.lastattrchange = 0;
		break;
#endif
*/
	}
	p->parents = NULL;
	nodepos = NODEHASHPOS(p->id);
	p->next = nodehash[nodepos];
	nodehash[nodepos] = p;
	p->checksum = 0;
	fsnodes_update_checksum(p);
	fsnodes_link(ts,node,p,nleng,name);
#ifndef METARESTORE
	fsnodes_quota_register_inode(p);
	if (type == TYPE_FILE) {
		fsnodes_quota_update_size(p, +fsnodes_get_size(p));
	}
#endif
	return p;
}

static inline uint32_t fsnodes_getpath_size(fsedge *e) {
	uint32_t size;
	fsnode *p;
	if (e==NULL) {
		return 0;
	}
	p = e->parent;
	size = e->nleng;
	while (p!=root && p->parents) {
		size += p->parents->nleng+1;
		p = p->parents->parent;
	}
	return size;
}

static inline void fsnodes_getpath_data(fsedge *e,uint8_t *path,uint32_t size) {
	fsnode *p;
	if (e==NULL) {
		return;
	}
	if (size>=e->nleng) {
		size-=e->nleng;
		memcpy(path+size,e->name,e->nleng);
	} else if (size>0) {
		memcpy(path,e->name+(e->nleng-size),size);
		size=0;
	}
	if (size>0) {
		path[--size]='/';
	}
	p = e->parent;
	while (p!=root && p->parents) {
		if (size>=p->parents->nleng) {
			size-=p->parents->nleng;
			memcpy(path+size,p->parents->name,p->parents->nleng);
		} else if (size>0) {
			memcpy(path,p->parents->name+(p->parents->nleng-size),size);
			size=0;
		}
		if (size>0) {
			path[--size]='/';
		}
		p = p->parents->parent;
	}
}

static inline void fsnodes_getpath(fsedge *e,uint16_t *pleng,uint8_t **path) {
	uint32_t size;
	uint8_t *ret;
	fsnode *p;

	p = e->parent;
	size = e->nleng;
	while (p!=root && p->parents) {
		size += p->parents->nleng+1;    // get first parent !!!
		p = p->parents->parent;         // when folders can be hardlinked it's the only way to obtain path (one of them)
	}
	if (size>65535) {
		syslog(LOG_WARNING,"path too long !!! - truncate");
		size=65535;
	}
	*pleng = size;
	ret = (uint8_t*) malloc(size);
	passert(ret);
	size -= e->nleng;
	memcpy(ret+size,e->name,e->nleng);
	if (size>0) {
		ret[--size]='/';
	}
	p = e->parent;
	while (p!=root && p->parents) {
		if (size>=p->parents->nleng) {
			size-=p->parents->nleng;
			memcpy(ret+size,p->parents->name,p->parents->nleng);
		} else {
			if (size>0) {
				memcpy(ret,p->parents->name+(p->parents->nleng-size),size);
				size=0;
			}
		}
		if (size>0) {
			ret[--size]='/';
		}
		p = p->parents->parent;
	}
	*path = ret;
}


#ifndef METARESTORE

static inline uint32_t fsnodes_getdetachedsize(fsedge *start) {
	fsedge *e;
	uint32_t result=0;
	for (e = start ; e ; e=e->nextchild) {
		if (e->nleng>240) {
			result+=245;
		} else {
			result+=5+e->nleng;
		}
	}
	return result;
}

static inline void fsnodes_getdetacheddata(fsedge *start,uint8_t *dbuff) {
	fsedge *e;
	uint8_t *sptr;
	uint8_t c;
	for (e = start ; e ; e=e->nextchild) {
		if (e->nleng>240) {
			*dbuff=240;
			dbuff++;
			memcpy(dbuff,"(...)",5);
			dbuff+=5;
			sptr = e->name+(e->nleng-235);
			for (c=0 ; c<235 ; c++) {
				if (*sptr=='/') {
					*dbuff='|';
				} else {
					*dbuff = *sptr;
				}
				sptr++;
				dbuff++;
			}
		} else {
			*dbuff=e->nleng;
			dbuff++;
			sptr = e->name;
			for (c=0 ; c<e->nleng ; c++) {
				if (*sptr=='/') {
					*dbuff='|';
				} else {
					*dbuff = *sptr;
				}
				sptr++;
				dbuff++;
			}
		}
		put32bit(&dbuff,e->child->id);
	}
}


static inline uint32_t fsnodes_getdirsize(fsnode *p,uint8_t withattr) {
	uint32_t result = ((withattr)?40:6)*2+3;        // for '.' and '..'
	fsedge *e;
	for (e = p->data.ddata.children ; e ; e=e->nextchild) {
		result+=((withattr)?40:6)+e->nleng;
	}
	return result;
}

static inline void fsnodes_getdirdata(uint32_t rootinode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t sesflags,fsnode *p,uint8_t *dbuff,uint8_t withattr) {
	fsedge *e;
// '.' - self
	dbuff[0]=1;
	dbuff[1]='.';
	dbuff+=2;
	if (p->id!=rootinode) {
		put32bit(&dbuff,p->id);
	} else {
		put32bit(&dbuff,MFS_ROOT_ID);
	}
	if (withattr) {
		fsnodes_fill_attr(p,p,uid,gid,auid,agid,sesflags,dbuff);
		dbuff+=35;
	} else {
		put8bit(&dbuff,TYPE_DIRECTORY);
	}
// '..' - parent
	dbuff[0]=2;
	dbuff[1]='.';
	dbuff[2]='.';
	dbuff+=3;
	if (p->id==rootinode) { // root node should returns self as its parent
		put32bit(&dbuff,MFS_ROOT_ID);
		if (withattr) {
			fsnodes_fill_attr(p,p,uid,gid,auid,agid,sesflags,dbuff);
			dbuff+=35;
		} else {
			put8bit(&dbuff,TYPE_DIRECTORY);
		}
	} else {
		if (p->parents && p->parents->parent->id!=rootinode) {
			put32bit(&dbuff,p->parents->parent->id);
		} else {
			put32bit(&dbuff,MFS_ROOT_ID);
		}
		if (withattr) {
			if (p->parents) {
				fsnodes_fill_attr(p->parents->parent,p,uid,gid,auid,agid,sesflags,dbuff);
			} else {
				if (rootinode==MFS_ROOT_ID) {
					fsnodes_fill_attr(root,p,uid,gid,auid,agid,sesflags,dbuff);
				} else {
					fsnode *rn = fsnodes_id_to_node(rootinode);
					if (rn) {       // it should be always true because it's checked before, but better check than sorry
						fsnodes_fill_attr(rn,p,uid,gid,auid,agid,sesflags,dbuff);
					} else {
						memset(dbuff,0,35);
					}
				}
			}
			dbuff+=35;
		} else {
			put8bit(&dbuff,TYPE_DIRECTORY);
		}
	}
// entries
	for (e = p->data.ddata.children ; e ; e=e->nextchild) {
		dbuff[0]=e->nleng;
		dbuff++;
		memcpy(dbuff,e->name,e->nleng);
		dbuff+=e->nleng;
		put32bit(&dbuff,e->child->id);
		if (withattr) {
			fsnodes_fill_attr(e->child,p,uid,gid,auid,agid,sesflags,dbuff);
			dbuff+=35;
		} else {
			put8bit(&dbuff,e->child->type);
		}
	}
}

static inline void fsnodes_checkfile(fsnode *p,uint32_t chunkcount[11]) {
	uint32_t i;
	uint64_t chunkid;
	uint8_t count;
	for (i=0 ; i<11 ; i++) {
		chunkcount[i]=0;
	}
	for (i=0 ; i<p->data.fdata.chunks ; i++) {
		chunkid = p->data.fdata.chunktab[i];
		if (chunkid>0) {
			chunk_get_validcopies(chunkid,&count);
			if (count>10) {
				count=10;
			}
			chunkcount[count]++;
		}
	}
}
#endif

static inline uint8_t fsnodes_appendchunks(uint32_t ts,fsnode *dstobj,fsnode *srcobj) {
	uint64_t chunkid,length;
	uint32_t i;
	uint32_t srcchunks,dstchunks;
#ifndef METARESTORE
	statsrecord psr,nsr;
	fsedge *e;
#endif

	srcchunks=0;
	for (i=0 ; i<srcobj->data.fdata.chunks ; i++) {
		if (srcobj->data.fdata.chunktab[i]!=0) {
			srcchunks = i+1;
		}
	}
	if (srcchunks==0) {
		return STATUS_OK;
	}
	dstchunks=0;
	for (i=0 ; i<dstobj->data.fdata.chunks ; i++) {
		if (dstobj->data.fdata.chunktab[i]!=0) {
			dstchunks = i+1;
		}
	}
	i = srcchunks+dstchunks-1;      // last new chunk pos
	if (i>MAX_INDEX) {      // chain too long
		return ERROR_INDEXTOOBIG;
	}
#ifndef METARESTORE
	fsnodes_get_stats(dstobj,&psr);
#endif
	if (i>=dstobj->data.fdata.chunks) {
		uint32_t newsize;
		if (i<8) {
			newsize=i+1;
		} else if (i<64) {
			newsize=(i&0xFFFFFFF8)+8;
		} else {
			newsize = (i&0xFFFFFFC0)+64;
		}
		if (dstobj->data.fdata.chunktab==NULL) {
			dstobj->data.fdata.chunktab = (uint64_t*)malloc(sizeof(uint64_t)*newsize);
		} else {
			dstobj->data.fdata.chunktab = (uint64_t*)realloc(dstobj->data.fdata.chunktab,sizeof(uint64_t)*newsize);
		}
		passert(dstobj->data.fdata.chunktab);
		for (i=dstobj->data.fdata.chunks ; i<newsize ; i++) {
			dstobj->data.fdata.chunktab[i]=0;
		}
		dstobj->data.fdata.chunks = newsize;
	}

	for (i=0 ; i<srcchunks ; i++) {
		chunkid = srcobj->data.fdata.chunktab[i];
		dstobj->data.fdata.chunktab[i+dstchunks] = chunkid;
		if (chunkid>0) {
			if (chunk_add_file(chunkid,dstobj->goal)!=STATUS_OK) {
				syslog(LOG_ERR,"structure error - chunk %016" PRIX64 " not found (inode: %" PRIu32 " ; index: %" PRIu32 ")",chunkid,srcobj->id,i);
			}
		}
	}

	length = (((uint64_t)dstchunks)<<MFSCHUNKBITS)+srcobj->data.fdata.length;
	if (dstobj->type==TYPE_TRASH) {
		trashspace -= dstobj->data.fdata.length;
		trashspace += length;
	} else if (dstobj->type==TYPE_RESERVED) {
		reservedspace -= dstobj->data.fdata.length;
		reservedspace += length;
	}
	dstobj->data.fdata.length = length;
#ifndef METARESTORE
	fsnodes_get_stats(dstobj,&nsr);
	for (e=dstobj->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	fsnodes_quota_update_size(dstobj, nsr.size - psr.size);
#endif
#ifdef METARESTORE
	dstobj->mtime = ts;
	dstobj->atime = ts;
	srcobj->atime = ts;
#else /* ! METARESTORE */
	dstobj->mtime = ts;
	dstobj->atime = ts;
/*
#ifdef CACHENOTIFY
	fsnodes_attr_changed(dstobj,ts);
#endif
*/
	if (srcobj->atime!=ts) {
		srcobj->atime = ts;
/*
#ifdef CACHENOTIFY
		fsnodes_attr_changed(srcobj,ts);
#endif
*/
	}
#endif
	fsnodes_update_checksum(srcobj);
	fsnodes_update_checksum(dstobj);
	return STATUS_OK;
}

static inline void fsnodes_changefilegoal(fsnode *obj,uint8_t goal) {
	uint32_t i;
	uint8_t old_goal = obj->goal;
#ifndef METARESTORE
	statsrecord psr,nsr;
	fsedge *e;

	fsnodes_get_stats(obj,&psr);
	obj->goal = goal;
	nsr = psr;
	nsr.realsize = file_realsize(obj, nsr.chunks, nsr.size);
	for (e=obj->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
#else
	obj->goal = goal;
#endif
	for (i=0 ; i<obj->data.fdata.chunks ; i++) {
		if (obj->data.fdata.chunktab[i]>0) {
			chunk_change_file(obj->data.fdata.chunktab[i],old_goal,goal);
		}
	}
	fsnodes_update_checksum(obj);
}

static inline void fsnodes_setlength(fsnode *obj,uint64_t length) {
	uint32_t i,chunks;
	uint64_t chunkid;
#ifndef METARESTORE
	fsedge *e;
	statsrecord psr,nsr;
	fsnodes_get_stats(obj,&psr);
#endif
	if (obj->type==TYPE_TRASH) {
		trashspace -= obj->data.fdata.length;
		trashspace += length;
	} else if (obj->type==TYPE_RESERVED) {
		reservedspace -= obj->data.fdata.length;
		reservedspace += length;
	}
	obj->data.fdata.length = length;
	if (length>0) {
		chunks = ((length-1)>>MFSCHUNKBITS)+1;
	} else {
		chunks = 0;
	}
	for (i=chunks ; i<obj->data.fdata.chunks ; i++) {
		chunkid = obj->data.fdata.chunktab[i];
		if (chunkid>0) {
			if (chunk_delete_file(chunkid,obj->goal)!=STATUS_OK) {
				syslog(LOG_ERR,"structure error - chunk %016" PRIX64 " not found (inode: %" PRIu32 " ; index: %" PRIu32 ")",chunkid,obj->id,i);
			}
		}
		obj->data.fdata.chunktab[i]=0;
	}
	if (chunks>0) {
		if (chunks<obj->data.fdata.chunks && obj->data.fdata.chunktab) {
			obj->data.fdata.chunktab = (uint64_t*)realloc(obj->data.fdata.chunktab,sizeof(uint64_t)*chunks);
			passert(obj->data.fdata.chunktab);
			obj->data.fdata.chunks = chunks;
		}
	} else {
		if (obj->data.fdata.chunks>0 && obj->data.fdata.chunktab) {
			free(obj->data.fdata.chunktab);
			obj->data.fdata.chunktab = NULL;
			obj->data.fdata.chunks = 0;
		}
	}
#ifndef METARESTORE
	fsnodes_get_stats(obj,&nsr);
	for (e=obj->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	fsnodes_quota_update_size(obj, nsr.size - psr.size);
#endif
	fsnodes_update_checksum(obj);
}


static inline void fsnodes_remove_node(uint32_t ts,fsnode *toremove) {
	uint32_t nodepos;
	fsnode **ptr;
	if (toremove->parents!=NULL) {
		return;
	}
// remove from idhash
	nodepos = NODEHASHPOS(toremove->id);
	ptr = &(nodehash[nodepos]);
	while (*ptr) {
		if (*ptr==toremove) {
			*ptr=toremove->next;
			break;
		}
		ptr = &((*ptr)->next);
	}
	removeFromChecksum(gFsNodesChecksum, toremove->checksum);
// and free
	nodes--;
	if (toremove->type==TYPE_DIRECTORY) {
		dirnodes--;
#ifndef METARESTORE
		free(toremove->data.ddata.stats);
/*
#ifdef CACHENOTIFY
		matoclserv_notify_remove(toremove->id);
#endif
*/
#endif
	}
	if (toremove->type==TYPE_FILE || toremove->type==TYPE_TRASH || toremove->type==TYPE_RESERVED) {
		uint32_t i;
		uint64_t chunkid;
#ifndef METARESTORE
		fsnodes_quota_update_size(toremove, -fsnodes_get_size(toremove));
#endif
		filenodes--;
		for (i=0 ; i<toremove->data.fdata.chunks ; i++) {
			chunkid = toremove->data.fdata.chunktab[i];
			if (chunkid>0) {
				if (chunk_delete_file(chunkid,toremove->goal)!=STATUS_OK) {
					syslog(LOG_ERR,"structure error - chunk %016" PRIX64 " not found (inode: %" PRIu32 " ; index: %" PRIu32 ")",chunkid,toremove->id,i);
				}
			}
		}
		if (toremove->data.fdata.chunktab!=NULL) {
			free(toremove->data.fdata.chunktab);
		}
	}
	if (toremove->type==TYPE_SYMLINK) {
		free(toremove->data.sdata.path);
	}
	fsnodes_free_id(toremove->id,ts);
	xattr_removeinode(toremove->id);
#ifndef METARESTORE
	dcm_modify(toremove->id,0);
	fsnodes_quota_unregister_inode(toremove);
#endif
	delete toremove;
}


static inline void fsnodes_unlink(uint32_t ts,fsedge *e) {
	fsnode *child;
	uint16_t pleng=0;
	uint8_t *path=NULL;

	child = e->child;
	if (child->parents->nextparent==NULL) { // last link
		if (child->type==TYPE_FILE && (child->trashtime>0 || child->data.fdata.sessionids!=NULL)) {     // go to trash or reserved ? - get path
			fsnodes_getpath(e,&pleng,&path);
		}
	}
	fsnodes_remove_edge(ts,e);
	if (child->parents==NULL) {     // last link
		if (child->type == TYPE_FILE) {
			if (child->trashtime>0) {
				child->type = TYPE_TRASH;
				child->ctime = ts;
				fsnodes_update_checksum(child);
				e = (fsedge*) malloc(sizeof(fsedge));
				passert(e);
				e->nleng = pleng;
				e->name = path;
				e->child = child;
				e->parent = NULL;
				e->nextchild = trash;
				e->nextparent = NULL;
				e->prevchild = &trash;
				e->prevparent = &(child->parents);
				if (e->nextchild) {
					e->nextchild->prevchild = &(e->nextchild);
				}
#ifdef EDGEHASH
				e->next = NULL;
				e->prev = NULL;
#endif
				trash = e;
				child->parents = e;
				trashspace += child->data.fdata.length;
				trashnodes++;
				e->checksum = 0;
				fsedges_update_checksum(e);
			} else if (child->data.fdata.sessionids!=NULL) {
				child->type = TYPE_RESERVED;
				fsnodes_update_checksum(child);
				e = (fsedge*) malloc(sizeof(fsedge));
				passert(e);
				e->nleng = pleng;
				e->name = path;
				e->child = child;
				e->parent = NULL;
				e->nextchild = reserved;
				e->nextparent = NULL;
				e->prevchild = &reserved;
				e->prevparent = &(child->parents);
				if (e->nextchild) {
					e->nextchild->prevchild = &(e->nextchild);
				}
#ifdef EDGEHASH
				e->next = NULL;
				e->prev = NULL;
#endif
				reserved = e;
				child->parents = e;
				reservedspace += child->data.fdata.length;
				reservednodes++;
				e->checksum = 0;
				fsedges_update_checksum(e);
			} else {
				free(path);
				fsnodes_remove_node(ts,child);
			}
		} else {
			free(path);
			fsnodes_remove_node(ts,child);
		}
	} else {
		free(path);
	}
}

static inline int fsnodes_purge(uint32_t ts,fsnode *p) {
	fsedge *e;
	e = p->parents;

	if (p->type==TYPE_TRASH) {
		trashspace -= p->data.fdata.length;
		trashnodes--;
		if (p->data.fdata.sessionids!=NULL) {
			p->type = TYPE_RESERVED;
			fsnodes_update_checksum(p);
			reservedspace += p->data.fdata.length;
			reservednodes++;
			*(e->prevchild) = e->nextchild;
			if (e->nextchild) {
				e->nextchild->prevchild = e->prevchild;
			}
			e->nextchild = reserved;
			e->prevchild = &(reserved);
			if (e->nextchild) {
				e->nextchild->prevchild = &(e->nextchild);
			}
			reserved = e;
			return 0;
		} else {
			fsnodes_remove_edge(ts,e);
			fsnodes_remove_node(ts,p);
			return 1;
		}
	} else if (p->type==TYPE_RESERVED) {
		reservedspace -= p->data.fdata.length;
		reservednodes--;
		fsnodes_remove_edge(ts,e);
		fsnodes_remove_node(ts,p);
		return 1;
	}
	return -1;
}

static inline uint8_t fsnodes_undel(uint32_t ts,fsnode *node) {
	uint16_t pleng;
	const uint8_t *path;
	uint8_t is_new;
	uint32_t i,partleng,dots;
	fsedge *e,*pe;
	fsnode *p,*n;

/* check path */
	e = node->parents;
	pleng = e->nleng;
	path = e->name;

	if (path==NULL) {
		return ERROR_CANTCREATEPATH;
	}
	while (*path=='/' && pleng>0) {
		path++;
		pleng--;
	}
	if (pleng==0) {
		return ERROR_CANTCREATEPATH;
	}
	partleng=0;
	dots=0;
	for (i=0 ; i<pleng ; i++) {
		if (path[i]==0) {       // incorrect name character
			return ERROR_CANTCREATEPATH;
		} else if (path[i]=='/') {
			if (partleng==0) {      // "//" in path
				return ERROR_CANTCREATEPATH;
			}
			if (partleng==dots && partleng<=2) {    // '.' or '..' in path
				return ERROR_CANTCREATEPATH;
			}
			partleng=0;
			dots=0;
		} else {
			if (path[i]=='.') {
				dots++;
			}
			partleng++;
			if (partleng>MAXFNAMELENG) {
				return ERROR_CANTCREATEPATH;
			}
		}
	}
	if (partleng==0) {      // last part canot be empty - it's the name of undeleted file
		return ERROR_CANTCREATEPATH;
	}
	if (partleng==dots && partleng<=2) {    // '.' or '..' in path
		return ERROR_CANTCREATEPATH;
	}

/* create path */
	n = NULL;
	p = root;
	is_new = 0;
	for (;;) {
		partleng=0;
		while (path[partleng]!='/' && partleng<pleng) {
			partleng++;
		}
		if (partleng==pleng) {  // last name
			if (fsnodes_nameisused(p,partleng,path)) {
				return ERROR_EEXIST;
			}
			// remove from trash and link to new parent
			node->type = TYPE_FILE;
			node->ctime = ts;
			fsnodes_update_checksum(node);
			fsnodes_link(ts,p,node,partleng,path);
			fsnodes_remove_edge(ts,e);
			trashspace -= node->data.fdata.length;
			trashnodes--;
			return STATUS_OK;
		} else {
			if (is_new==0) {
				pe = fsnodes_lookup(p,partleng,path);
				if (pe==NULL) {
					is_new=1;
				} else {
					n = pe->child;
					if (n->type!=TYPE_DIRECTORY) {
						return ERROR_CANTCREATEPATH;
					}
				}
			}
			if (is_new==1) {
				n = fsnodes_create_node(ts,p,partleng,path,TYPE_DIRECTORY,0755,0,0,0,0,AclInheritance::kDontInheritAcl);
			}
			p = n;
		}
		path+=partleng+1;
		pleng-=partleng+1;
	}
}


#ifndef METARESTORE

static inline void fsnodes_getgoal_recursive(fsnode *node, uint8_t gmode, GoalStats& goalStats) {
	fsedge *e;

	if (node->type == TYPE_FILE || node->type == TYPE_TRASH || node->type == TYPE_RESERVED) {
		if (isOrdinaryGoal(node->goal)) {
			goalStats.filesWithGoal[node->goal]++;
		} else if (isXorGoal(node->goal)) {
			goalStats.filesWithXorLevel[goalToXorLevel(node->goal)]++;
		} else {
			syslog(LOG_WARNING, "inode %" PRIu32 ": unknown goal!!! fixing", node->id);
			sassert(node->parents);
			sassert(node->parents->parent);
			fsnodes_changefilegoal(node, node->parents->parent->goal);
		}
	} else if (node->type == TYPE_DIRECTORY) {
		if (isOrdinaryGoal(node->goal)) {
			goalStats.directoriesWithGoal[node->goal]++;
		} else if (isXorGoal(node->goal)) {
			goalStats.directoriesWithXorLevel[goalToXorLevel(node->goal)]++;
		} else {
			syslog(LOG_WARNING,"inode %" PRIu32 ": unknown goal!!! fixing", node->id);
			if (node->id == 1) {
				// Root node
				node->goal = 1;
			} else {
				sassert(node->parents);
				sassert(node->parents->parent);
				node->goal = node->parents->parent->goal;
			}
		}
		if (gmode == GMODE_RECURSIVE) {
			for (e = node->data.ddata.children; e; e = e->nextchild) {
				fsnodes_getgoal_recursive(e->child, gmode, goalStats);
			}
		}
	}
}

static inline void fsnodes_bst_add(bstnode **n,uint32_t val) {
	while (*n) {
		if (val<(*n)->val) {
			n = &((*n)->left);
		} else if (val>(*n)->val) {
			n = &((*n)->right);
		} else {
			(*n)->count++;
			return;
		}
	}
	(*n)= (bstnode*) malloc(sizeof(bstnode));
	passert(*n);
	(*n)->val = val;
	(*n)->count = 1;
	(*n)->left = NULL;
	(*n)->right = NULL;
}

static inline uint32_t fsnodes_bst_nodes(bstnode *n) {
	if (n) {
		return 1+fsnodes_bst_nodes(n->left)+fsnodes_bst_nodes(n->right);
	} else {
		return 0;
	}
}

static inline void fsnodes_bst_storedata(bstnode *n,uint8_t **ptr) {
	if (n) {
		fsnodes_bst_storedata(n->left,ptr);
		put32bit(&*ptr,n->val);
		put32bit(&*ptr,n->count);
		fsnodes_bst_storedata(n->right,ptr);
	}
}

static inline void fsnodes_bst_free(bstnode *n) {
	if (n) {
		fsnodes_bst_free(n->left);
		fsnodes_bst_free(n->right);
		free(n);
	}
}

static inline void fsnodes_gettrashtime_recursive(fsnode *node,uint8_t gmode,bstnode **bstrootfiles,bstnode **bstrootdirs) {
	fsedge *e;

	if (node->type==TYPE_FILE || node->type==TYPE_TRASH || node->type==TYPE_RESERVED) {
		fsnodes_bst_add(bstrootfiles,node->trashtime);
	} else if (node->type==TYPE_DIRECTORY) {
		fsnodes_bst_add(bstrootdirs,node->trashtime);
		if (gmode==GMODE_RECURSIVE) {
			for (e = node->data.ddata.children ; e ; e=e->nextchild) {
				fsnodes_gettrashtime_recursive(e->child,gmode,bstrootfiles,bstrootdirs);
			}
		}
	}
}

static inline void fsnodes_geteattr_recursive(fsnode *node,uint8_t gmode,uint32_t feattrtab[16],uint32_t deattrtab[16]) {
	fsedge *e;

	if (node->type!=TYPE_DIRECTORY) {
		feattrtab[(node->mode>>12)&(EATTR_NOOWNER|EATTR_NOACACHE|EATTR_NODATACACHE)]++;
	} else {
		deattrtab[(node->mode>>12)]++;
		if (gmode==GMODE_RECURSIVE) {
			for (e = node->data.ddata.children ; e ; e=e->nextchild) {
				fsnodes_geteattr_recursive(e->child,gmode,feattrtab,deattrtab);
			}
		}
	}
}

#endif

static inline void fsnodes_setgoal_recursive(fsnode *node, uint32_t ts, uint32_t uid, uint8_t goal,
		uint8_t smode, uint32_t *sinodes, uint32_t *ncinodes, uint32_t *nsinodes) {
	fsedge *e;

	if (node->type==TYPE_FILE
			|| node->type==TYPE_DIRECTORY
			|| node->type==TYPE_TRASH
			|| node->type==TYPE_RESERVED) {
		if ((node->mode & (EATTR_NOOWNER << 12)) == 0 && uid != 0 && node->uid != uid) {
			(*nsinodes)++;
		} else {
			bool set = false;
			switch (smode & SMODE_TMASK) {
			case SMODE_SET:
				set = (node->goal != goal);
				break;
			case SMODE_INCREASE:
				sassert(isOrdinaryGoal(goal));
				set = (isOrdinaryGoal(node->goal) && node->goal < goal);
				break;
			case SMODE_DECREASE:
				sassert(isOrdinaryGoal(goal));
				set = (isOrdinaryGoal(node->goal) && node->goal > goal);
				break;
			}
			if (set) {
				if (node->type!=TYPE_DIRECTORY) {
					fsnodes_changefilegoal(node,goal);
					(*sinodes)++;
				} else {
					node->goal = goal;
					(*sinodes)++;
				}
				node->ctime = ts;
				fsnodes_update_checksum(node);
/*
#ifndef METARESTORE
#ifdef CACHENOTIFY
				fsnodes_attr_changed(node,ts);
#endif
#endif
*/
			} else {
				(*ncinodes)++;
			}
		}
		if (node->type == TYPE_DIRECTORY && (smode & SMODE_RMASK)) {
			for (e = node->data.ddata.children ; e ; e=e->nextchild) {
				fsnodes_setgoal_recursive(e->child,ts,uid,goal,smode,sinodes,ncinodes,nsinodes);
			}
		}
	}
}

static inline void fsnodes_settrashtime_recursive(fsnode *node,uint32_t ts,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	fsedge *e;
	uint8_t set;

	if (node->type==TYPE_FILE || node->type==TYPE_DIRECTORY || node->type==TYPE_TRASH || node->type==TYPE_RESERVED) {
		if ((node->mode&(EATTR_NOOWNER<<12))==0 && uid!=0 && node->uid!=uid) {
			(*nsinodes)++;
		} else {
			set=0;
			switch (smode&SMODE_TMASK) {
			case SMODE_SET:
				if (node->trashtime!=trashtime) {
					node->trashtime=trashtime;
					set=1;
				}
				break;
			case SMODE_INCREASE:
				if (node->trashtime<trashtime) {
					node->trashtime=trashtime;
					set=1;
				}
				break;
			case SMODE_DECREASE:
				if (node->trashtime>trashtime) {
					node->trashtime=trashtime;
					set=1;
				}
				break;
			}
			if (set) {
				(*sinodes)++;
				node->ctime = ts;
				fsnodes_update_checksum(node);
/*
#ifndef METARESTORE
#ifdef CACHENOTIFY
				fsnodes_attr_changed(node,ts);
#endif
#endif
*/
			} else {
				(*ncinodes)++;
			}
		}
		if (node->type==TYPE_DIRECTORY && (smode&SMODE_RMASK)) {
			for (e = node->data.ddata.children ; e ; e=e->nextchild) {
				fsnodes_settrashtime_recursive(e->child,ts,uid,trashtime,smode,sinodes,ncinodes,nsinodes);
			}
		}
	}
}

static inline void fsnodes_seteattr_recursive(fsnode *node,uint32_t ts,uint32_t uid,uint8_t eattr,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	fsedge *e;
	uint8_t neweattr,seattr;

	if ((node->mode&(EATTR_NOOWNER<<12))==0 && uid!=0 && node->uid!=uid) {
		(*nsinodes)++;
	} else {
		seattr = eattr;
		if (node->type!=TYPE_DIRECTORY) {
			node->mode &= ~(EATTR_NOECACHE<<12);
			seattr &= ~(EATTR_NOECACHE);
		}
		neweattr = (node->mode>>12);
		switch (smode&SMODE_TMASK) {
			case SMODE_SET:
				neweattr = seattr;
				break;
			case SMODE_INCREASE:
				neweattr |= seattr;
				break;
			case SMODE_DECREASE:
				neweattr &= ~seattr;
				break;
		}
		if (neweattr!=(node->mode>>12)) {
			node->mode = (node->mode&0xFFF) | (((uint16_t)neweattr)<<12);
			(*sinodes)++;
			node->ctime = ts;
/*
#ifndef METARESTORE
#ifdef CACHENOTIFY
			fsnodes_attr_changed(node,ts);
#endif
#endif
*/
		} else {
			(*ncinodes)++;
		}
	}
	if (node->type==TYPE_DIRECTORY && (smode&SMODE_RMASK)) {
		for (e = node->data.ddata.children ; e ; e=e->nextchild) {
			fsnodes_seteattr_recursive(e->child,ts,uid,eattr,smode,sinodes,ncinodes,nsinodes);
		}
	}
	fsnodes_update_checksum(node);
}

/// creates cow copy
static inline void fsnodes_snapshot(uint32_t ts,fsnode *srcnode,fsnode *parentnode,uint32_t nleng,const uint8_t *name) {
	fsedge *e;
	fsnode *dstnode = nullptr;
	uint32_t i;
	uint64_t chunkid;
	if ((e=fsnodes_lookup(parentnode,nleng,name))) {
		// link already exists
		dstnode = e->child;
		if (srcnode->type==TYPE_DIRECTORY) {
			for (e = srcnode->data.ddata.children ; e ; e=e->nextchild) {
				fsnodes_snapshot(ts,e->child,dstnode,e->nleng,e->name);
			}
		} else if (srcnode->type==TYPE_FILE) {
			uint8_t same = 0;
			if (dstnode->data.fdata.length==srcnode->data.fdata.length && dstnode->data.fdata.chunks==srcnode->data.fdata.chunks) {
				same=1;
				for (i=0 ; i<srcnode->data.fdata.chunks && same ; i++) {
					if (srcnode->data.fdata.chunktab[i]!=dstnode->data.fdata.chunktab[i]) {
						same=0;
					}
				}
			}
			if (same==0) {
#ifndef METARESTORE
				statsrecord psr,nsr;
#endif
				fsnodes_unlink(ts,e);
				dstnode = fsnodes_create_node(ts,parentnode,nleng,name,TYPE_FILE,srcnode->mode,0,srcnode->uid,srcnode->gid,0,AclInheritance::kDontInheritAcl);
#ifndef METARESTORE
				fsnodes_get_stats(dstnode,&psr);
#endif
				dstnode->goal = srcnode->goal;
				dstnode->trashtime = srcnode->trashtime;
				if (srcnode->data.fdata.chunks>0) {
					dstnode->data.fdata.chunktab = (uint64_t*)malloc(sizeof(uint64_t)*(srcnode->data.fdata.chunks));
					passert(dstnode->data.fdata.chunktab);
					dstnode->data.fdata.chunks = srcnode->data.fdata.chunks;
					for (i=0 ; i<srcnode->data.fdata.chunks ; i++) {
						chunkid = srcnode->data.fdata.chunktab[i];
						dstnode->data.fdata.chunktab[i] = chunkid;
						if (chunkid>0) {
							if (chunk_add_file(chunkid,dstnode->goal)!=STATUS_OK) {
								syslog(LOG_ERR,"structure error - chunk %016" PRIX64 " not found (inode: %" PRIu32 " ; index: %" PRIu32 ")",chunkid,srcnode->id,i);
							}
						}
					}
				} else {
					dstnode->data.fdata.chunktab = NULL;
					dstnode->data.fdata.chunks = 0;
				}
				dstnode->data.fdata.length = srcnode->data.fdata.length;
#ifndef METARESTORE
				fsnodes_get_stats(dstnode,&nsr);
				fsnodes_add_sub_stats(parentnode,&nsr,&psr);
				fsnodes_quota_update_size(dstnode, nsr.size - psr.size);
/*
#ifdef CACHENOTIFY
				if (dstnode->data.fdata.length>0) {
					fsnodes_attr_changed(dstnode,ts);
				}
#endif
*/
#endif
			}
		} else if (srcnode->type==TYPE_SYMLINK) {
#ifndef METARESTORE
			if (dstnode->data.sdata.pleng!=srcnode->data.sdata.pleng) {
				statsrecord sr;
				memset(&sr,0,sizeof(statsrecord));
				sr.length = dstnode->data.sdata.pleng-srcnode->data.sdata.pleng;
				fsnodes_add_stats(parentnode,&sr);
			}
#endif
			if (dstnode->data.sdata.path) {
				free(dstnode->data.sdata.path);
			}
			if (srcnode->data.sdata.pleng>0) {
				dstnode->data.sdata.path = (uint8_t*) malloc(srcnode->data.sdata.pleng);
				passert(dstnode->data.sdata.path);
				memcpy(dstnode->data.sdata.path,srcnode->data.sdata.path,srcnode->data.sdata.pleng);
				dstnode->data.sdata.pleng = srcnode->data.sdata.pleng;
			} else {
				dstnode->data.sdata.path=NULL;
				dstnode->data.sdata.pleng=0;
			}
		} else if (srcnode->type==TYPE_BLOCKDEV || srcnode->type==TYPE_CHARDEV) {
			dstnode->data.devdata.rdev = srcnode->data.devdata.rdev;
		}
		dstnode->mode = srcnode->mode;
		dstnode->uid = srcnode->uid;
		dstnode->gid = srcnode->gid;
		dstnode->atime = srcnode->atime;
		dstnode->mtime = srcnode->mtime;
		dstnode->ctime = ts;
#ifndef METARESTORE
/*
#ifdef CACHENOTIFY
		fsnodes_attr_changed(dstnode,ts);
#endif
*/
#endif
	} else {
		if (srcnode->type==TYPE_FILE || srcnode->type==TYPE_DIRECTORY || srcnode->type==TYPE_SYMLINK || srcnode->type==TYPE_BLOCKDEV || srcnode->type==TYPE_CHARDEV || srcnode->type==TYPE_SOCKET || srcnode->type==TYPE_FIFO) {
#ifndef METARESTORE
			statsrecord psr,nsr;
#endif
			dstnode = fsnodes_create_node(ts,parentnode,nleng,name,srcnode->type,srcnode->mode,0,srcnode->uid,srcnode->gid,0,AclInheritance::kDontInheritAcl);
#ifndef METARESTORE
			fsnodes_get_stats(dstnode,&psr);
#endif
			dstnode->goal = srcnode->goal;
			dstnode->trashtime = srcnode->trashtime;
			dstnode->mode = srcnode->mode;
			dstnode->atime = srcnode->atime;
			dstnode->mtime = srcnode->mtime;
			if (srcnode->type==TYPE_DIRECTORY) {
				for (e = srcnode->data.ddata.children ; e ; e=e->nextchild) {
					fsnodes_snapshot(ts,e->child,dstnode,e->nleng,e->name);
				}
			} else if (srcnode->type==TYPE_FILE) {
				if (srcnode->data.fdata.chunks>0) {
					dstnode->data.fdata.chunktab = (uint64_t*)malloc(sizeof(uint64_t)*(srcnode->data.fdata.chunks));
					passert(dstnode->data.fdata.chunktab);
					dstnode->data.fdata.chunks = srcnode->data.fdata.chunks;
					for (i=0 ; i<srcnode->data.fdata.chunks ; i++) {
						chunkid = srcnode->data.fdata.chunktab[i];
						dstnode->data.fdata.chunktab[i] = chunkid;
						if (chunkid>0) {
							if (chunk_add_file(chunkid,dstnode->goal)!=STATUS_OK) {
								syslog(LOG_ERR,"structure error - chunk %016" PRIX64 " not found (inode: %" PRIu32 " ; index: %" PRIu32 ")",chunkid,srcnode->id,i);
							}
						}
					}
				} else {
					dstnode->data.fdata.chunktab = NULL;
					dstnode->data.fdata.chunks = 0;
				}
				dstnode->data.fdata.length = srcnode->data.fdata.length;
#ifndef METARESTORE
				fsnodes_get_stats(dstnode,&nsr);
				fsnodes_add_sub_stats(parentnode,&nsr,&psr);
				fsnodes_quota_update_size(dstnode, nsr.size - psr.size);
#endif
			} else if (srcnode->type==TYPE_SYMLINK) {
				if (srcnode->data.sdata.pleng>0) {
					dstnode->data.sdata.path = (uint8_t*) malloc(srcnode->data.sdata.pleng);
					passert(dstnode->data.sdata.path);
					memcpy(dstnode->data.sdata.path,srcnode->data.sdata.path,srcnode->data.sdata.pleng);
					dstnode->data.sdata.pleng = srcnode->data.sdata.pleng;
				}
#ifndef METARESTORE
				fsnodes_get_stats(dstnode,&nsr);
				fsnodes_add_sub_stats(parentnode,&nsr,&psr);
#endif
			} else if (srcnode->type==TYPE_BLOCKDEV || srcnode->type==TYPE_CHARDEV) {
				dstnode->data.devdata.rdev = srcnode->data.devdata.rdev;
			}
#ifndef METARESTORE
/*
#ifdef CACHENOTIFY
			fsnodes_attr_changed(dstnode,ts);
#endif
*/
#endif
		}
	}
	fsnodes_update_checksum(dstnode);
}

static inline uint8_t fsnodes_snapshot_test(fsnode *origsrcnode,fsnode *srcnode,fsnode *parentnode,uint32_t nleng,const uint8_t *name,uint8_t canoverwrite) {
	fsedge *e;
	fsnode *dstnode;
	uint8_t status;
#ifndef METARESTORE
	if (fsnodes_inode_quota_exceeded(srcnode->uid, srcnode->gid)) {
		return ERROR_QUOTA;
	}
	if (srcnode->type == TYPE_FILE && fsnodes_size_quota_exceeded(srcnode->uid, srcnode->gid)) {
		return ERROR_QUOTA;
	}
#endif
	if ((e=fsnodes_lookup(parentnode,nleng,name))) {
		dstnode = e->child;
		if (dstnode==origsrcnode) {
			return ERROR_EINVAL;
		}
		if (dstnode->type!=srcnode->type) {
			return ERROR_EPERM;
		}
		if (srcnode->type==TYPE_TRASH || srcnode->type==TYPE_RESERVED) {
			return ERROR_EPERM;
		}
		if (srcnode->type==TYPE_DIRECTORY) {
			for (e = srcnode->data.ddata.children ; e ; e=e->nextchild) {
				status = fsnodes_snapshot_test(origsrcnode,e->child,dstnode,e->nleng,e->name,canoverwrite);
				if (status!=STATUS_OK) {
					return status;
				}
			}
		} else if (canoverwrite==0) {
			return ERROR_EEXIST;
		}
	}
	return STATUS_OK;
}

static uint8_t fsnodes_deleteacl(fsnode *p, AclType type, uint32_t ts) {
	if (type == AclType::kDefault) {
		if (p->type != TYPE_DIRECTORY) {
			return ERROR_ENOTSUP;
		}
		p->defaultAcl.reset();
	} else {
		p->extendedAcl.reset();
	}
	p->ctime = ts;
	fsnodes_update_checksum(p);
	return STATUS_OK;
}

#ifndef METARESTORE
static uint8_t fsnodes_getacl(fsnode *p, AclType type, AccessControlList& acl) {
	if (type == AclType::kDefault) {
		if (p->type != TYPE_DIRECTORY || !p->defaultAcl) {
			return ERROR_ENOATTR;
		}
		acl = *(p->defaultAcl);
	} else {
		if (!p->extendedAcl) {
			return ERROR_ENOATTR;
		}
		acl.mode = (p->mode & 0777);
		acl.extendedAcl.reset(new ExtendedAcl(*p->extendedAcl));
	}
	return STATUS_OK;
}
#endif

static uint8_t fsnodes_setacl(fsnode *p, AclType type, AccessControlList acl, uint32_t ts) {
	if (type == AclType::kDefault) {
		if (p->type != TYPE_DIRECTORY) {
			return ERROR_ENOTSUP;
		}
		p->defaultAcl.reset(new AccessControlList(std::move(acl)));
	} else {
		p->mode = (p->mode & ~0777) | (acl.mode & 0777);
		p->extendedAcl = std::move(acl.extendedAcl);
	}
	p->ctime = ts;
	fsnodes_update_checksum(p);
	return STATUS_OK;
}

static inline int fsnodes_namecheck(uint32_t nleng,const uint8_t *name) {
	uint32_t i;
	if (nleng==0 || nleng>MAXFNAMELENG) {
		return -1;
	}
	if (name[0]=='.') {
		if (nleng==1) {
			return -1;
		}
		if (nleng==2 && name[1]=='.') {
			return -1;
		}
	}
	for (i=0 ; i<nleng ; i++) {
		if (name[i]=='\0' || name[i]=='/') {
			return -1;
		}
	}
	return 0;
}

#ifndef METARESTORE
static inline int fsnodes_access(fsnode *node,uint32_t uid,uint32_t gid,uint8_t modemask,uint8_t sesflags) {
	uint8_t nodemode;
	if ((sesflags & SESFLAG_NOMASTERPERMCHECK) || uid==0) {
		return 1;
	}
	if (uid==node->uid || (node->mode&(EATTR_NOOWNER<<12))) {
		nodemode = ((node->mode)>>6) & 7;
	} else if (sesflags&SESFLAG_IGNOREGID) {
		nodemode = (((node->mode)>>3) | (node->mode)) & 7;
	} else if (gid==node->gid) {
		nodemode = ((node->mode)>>3) & 7;
	} else {
		nodemode = (node->mode & 7);
	}
	if ((nodemode & modemask) == modemask) {
		return 1;
	}
	return 0;
}

static inline int fsnodes_sticky_access(fsnode *parent,fsnode *node,uint32_t uid) {
	if (uid==0 || (parent->mode&01000)==0) {        // super user or sticky bit is not set
		return 1;
	}
	if (uid==parent->uid || (parent->mode&(EATTR_NOOWNER<<12)) || uid==node->uid || (node->mode&(EATTR_NOOWNER<<12))) {
		return 1;
	}
	return 0;
}

// Arguments for fsnodes_get_node_for_operation
namespace {
	enum class ExpectedInodeType { kOnlyLinked, kAny };
	enum class OperationType { kReadOnly, kChangesMetadata };
}

// Treating rootinode as the root of the hierarchy, converts (rootinode, inode) to (inode, fsnode*)
// ie:
// * if inode == rootinode, then returns (rootinode, root node)
// * if inode != rootinode, then returns (inode, some node)
// Checks for permissions needed to perform the operation (defined by modemask and operationType)
// Can return a reserved node or a node from trash iff inodeType == ExpectedInodeType::kAny
static uint8_t fsnodes_get_node_for_operation(uint32_t rootinode, uint8_t sesflags,
		uint32_t uid, uint32_t gid, uint8_t modemask,
		OperationType operationType, ExpectedInodeType inodeType,
		uint32_t *inode, fsnode **ret) {
	sassert(!(operationType == OperationType::kReadOnly && (modemask & MODE_MASK_W)));
	if ((operationType != OperationType::kReadOnly) && (sesflags & SESFLAG_READONLY)) {
		return ERROR_EROFS;
	}
	fsnode *p;
	if (rootinode == MFS_ROOT_ID || (rootinode == 0 && inodeType == ExpectedInodeType::kAny)) {
		p = fsnodes_id_to_node(*inode);
		if (!p) {
			return ERROR_ENOENT;
		}
		if (rootinode == 0 && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
			return ERROR_EPERM;
		}
	} else {
		fsnode *rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (*inode == MFS_ROOT_ID) {
			*inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(*inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn, p)) {
				return ERROR_EPERM;
			}
		}
	}
	if (!fsnodes_access(p, uid, gid, modemask, sesflags)) {
		return ERROR_EACCES;
	}
	*ret = p;
	return STATUS_OK;
}
#endif

/* master <-> fuse operations */

#ifdef METARESTORE
template <class T>
bool decodeChar(const char *keys, const std::vector<T> values, char key, T& value) {
	const uint32_t count = strlen(keys);
	sassert(values.size() == count);
	for (uint32_t i = 0; i < count; i++) {
		if (key == keys[i]) {
			value = values[i];
			return true;
		}
	}
	return false;
}
#endif

#ifdef METARESTORE
uint8_t fs_access(uint32_t ts,uint32_t inode) {
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	p->atime = ts;
	metaversion++;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint8_t fs_readreserved_size(uint32_t rootinode,uint8_t sesflags,uint32_t *dbuffsize) {
	if (rootinode!=0) {
		return ERROR_EPERM;
	}
	(void)sesflags;
	*dbuffsize = fsnodes_getdetachedsize(reserved);
	return STATUS_OK;
}

void fs_readreserved_data(uint32_t rootinode,uint8_t sesflags,uint8_t *dbuff) {
	(void)rootinode;
	(void)sesflags;
	fsnodes_getdetacheddata(reserved,dbuff);
}


uint8_t fs_readtrash_size(uint32_t rootinode,uint8_t sesflags,uint32_t *dbuffsize) {
	if (rootinode!=0) {
		return ERROR_EPERM;
	}
	(void)sesflags;
	*dbuffsize = fsnodes_getdetachedsize(trash);
	return STATUS_OK;
}

void fs_readtrash_data(uint32_t rootinode,uint8_t sesflags,uint8_t *dbuff) {
	(void)rootinode;
	(void)sesflags;
	fsnodes_getdetacheddata(trash,dbuff);
}

/* common procedure for trash and reserved files */
uint8_t fs_getdetachedattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t attr[35],uint8_t dtype) {
	fsnode *p;
	memset(attr,0,35);
	if (rootinode!=0) {
		return ERROR_EPERM;
	}
	(void)sesflags;
	if (!DTYPE_ISVALID(dtype)) {
		return ERROR_EINVAL;
	}
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_ENOENT;
	}
	if (dtype==DTYPE_TRASH && p->type==TYPE_RESERVED) {
		return ERROR_ENOENT;
	}
	if (dtype==DTYPE_RESERVED && p->type==TYPE_TRASH) {
		return ERROR_ENOENT;
	}
	fsnodes_fill_attr(p,NULL,p->uid,p->gid,p->uid,p->gid,sesflags,attr);
	return STATUS_OK;
}

uint8_t fs_gettrashpath(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t *pleng,uint8_t **path) {
	fsnode *p;
	*pleng = 0;
	*path = NULL;
	if (rootinode!=0) {
		return ERROR_EPERM;
	}
	(void)sesflags;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_TRASH) {
		return ERROR_ENOENT;
	}
	*pleng = p->parents->nleng;
	*path = p->parents->name;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint8_t fs_settrashpath(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t pleng,const uint8_t *path) {
#else
uint8_t fs_setpath(uint32_t inode,const uint8_t *path) {
	uint32_t pleng;
#endif
	fsnode *p;
	uint8_t *newpath;
#ifdef METARESTORE
	pleng = strlen((char*)path);
#else
	uint32_t i;
	if (rootinode!=0) {
		return ERROR_EPERM;
	}
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (pleng==0) {
		return ERROR_EINVAL;
	}
	for (i=0 ; i<pleng ; i++) {
		if (path[i]==0) {
			return ERROR_EINVAL;
		}
	}
#endif
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_TRASH) {
		return ERROR_ENOENT;
	}
	newpath = (uint8_t*) malloc(pleng);
	passert(newpath);
	free(p->parents->name);
	memcpy(newpath,path,pleng);
	p->parents->name = newpath;
	p->parents->nleng = pleng;
	fsedges_update_checksum(p->parents);
#ifndef METARESTORE
	changelog(metaversion++,"%" PRIu32 "|SETPATH(%" PRIu32 ",%s)",(uint32_t)main_time(),inode,fsnodes_escape_name(pleng,newpath));
#else
	metaversion++;
#endif
	return STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_undel(uint32_t rootinode,uint8_t sesflags,uint32_t inode) {
	uint32_t ts;
#else
uint8_t fs_undel(uint32_t ts,uint32_t inode) {
#endif
	fsnode *p;
	uint8_t status;
#ifndef METARESTORE
	if (rootinode!=0) {
		return ERROR_EPERM;
	}
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	ts = main_time();
#endif
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_TRASH) {
		return ERROR_ENOENT;
	}
	status = fsnodes_undel(ts,p);
#ifndef METARESTORE
	if (status==STATUS_OK) {
		changelog(metaversion++,"%" PRIu32 "|UNDEL(%" PRIu32 ")",ts,inode);
	}
#else
	metaversion++;
#endif
	return status;
}

#ifndef METARESTORE
uint8_t fs_purge(uint32_t rootinode,uint8_t sesflags,uint32_t inode) {
	uint32_t ts;
#else
uint8_t fs_purge(uint32_t ts,uint32_t inode) {
#endif
	fsnode *p;
#ifndef METARESTORE
	if (rootinode!=0) {
		return ERROR_EPERM;
	}
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	ts = main_time();
#endif
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_TRASH) {
		return ERROR_ENOENT;
	}
	fsnodes_purge(ts,p);
#ifndef METARESTORE
	changelog(metaversion++,"%" PRIu32 "|PURGE(%" PRIu32 ")",ts,inode);
#else
	metaversion++;
#endif
	return STATUS_OK;
}

#ifndef METARESTORE
void fs_info(uint64_t *totalspace,uint64_t *availspace,uint64_t *trspace,uint32_t *trnodes,uint64_t *respace,uint32_t *renodes,uint32_t *inodes,uint32_t *dnodes,uint32_t *fnodes) {
	matocsserv_getspace(totalspace,availspace);
	*trspace = trashspace;
	*trnodes = trashnodes;
	*respace = reservedspace;
	*renodes = reservednodes;
	*inodes = nodes;
	*dnodes = dirnodes;
	*fnodes = filenodes;
}

uint8_t fs_getrootinode(uint32_t *rootinode,const uint8_t *path) {
	uint32_t nleng;
	const uint8_t *name;
	fsnode *p;
	fsedge *e;

	name = path;
	p = root;
	for (;;) {
		while (*name=='/') {
			name++;
		}
		if (*name=='\0') {
			*rootinode = p->id;
			return STATUS_OK;
		}
		nleng=0;
		while (name[nleng] && name[nleng]!='/') {
			nleng++;
		}
		if (fsnodes_namecheck(nleng,name)<0) {
			return ERROR_EINVAL;
		}
		e = fsnodes_lookup(p,nleng,name);
		if (!e) {
			return ERROR_ENOENT;
		}
		p = e->child;
		if (p->type!=TYPE_DIRECTORY) {
			return ERROR_ENOTDIR;
		}
		name += nleng;
	}
}

void fs_statfs(uint32_t rootinode,uint8_t sesflags,uint64_t *totalspace,uint64_t *availspace,uint64_t *trspace,uint64_t *respace,uint32_t *inodes) {
	fsnode *rn;
	statsrecord sr;
	(void)sesflags;
	if (rootinode==MFS_ROOT_ID) {
		*trspace = trashspace;
		*respace = reservedspace;
		rn = root;
	} else {
		*trspace = 0;
		*respace = 0;
		rn = fsnodes_id_to_node(rootinode);
	}
	if (!rn || rn->type!=TYPE_DIRECTORY) {
		*totalspace = 0;
		*availspace = 0;
		*inodes = 0;
	} else {
		matocsserv_getspace(totalspace,availspace);
		fsnodes_get_stats(rn,&sr);
		*inodes = sr.inodes;
		if (sr.realsize + *availspace < *totalspace) {
			*totalspace = sr.realsize + *availspace;
		}
	}
	stats_statfs++;
}

uint8_t fs_access(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,int modemask) {
	fsnode *p,*rn;
	if ((sesflags&SESFLAG_READONLY) && (modemask&MODE_MASK_W)) {
		return ERROR_EROFS;
	}
	if (rootinode==MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	return fsnodes_access(p,uid,gid,modemask,sesflags)?STATUS_OK:ERROR_EACCES;
}

uint8_t fs_lookup(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[35]) {
	fsnode *wd,*rn;
	fsedge *e;
/*
#ifdef CACHENOTIFY
	uint32_t ts = main_time();
#endif
*/

	*inode = 0;
	memset(attr,0,35);
	if (rootinode==MFS_ROOT_ID) {
		rn = root;
		wd = fsnodes_id_to_node(parent);
		if (!wd) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (parent==MFS_ROOT_ID) {
			parent = rootinode;
			wd = rn;
		} else {
			wd = fsnodes_id_to_node(parent);
			if (!wd) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,wd)) {
				return ERROR_EPERM;
			}
		}
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if (!fsnodes_access(wd,uid,gid,MODE_MASK_X,sesflags)) {
		return ERROR_EACCES;
	}
	if (name[0]=='.') {
		if (nleng==1) { // self
			if (parent==rootinode) {
				*inode = MFS_ROOT_ID;
			} else {
				*inode = wd->id;
			}
			fsnodes_fill_attr(wd,wd,uid,gid,auid,agid,sesflags,attr);
			stats_lookup++;
			return STATUS_OK;
		}
		if (nleng==2 && name[1]=='.') { // parent
			if (parent==rootinode) {
				*inode = MFS_ROOT_ID;
				fsnodes_fill_attr(wd,wd,uid,gid,auid,agid,sesflags,attr);
			} else {
				if (wd->parents) {
					if (wd->parents->parent->id==rootinode) {
						*inode = MFS_ROOT_ID;
					} else {
						*inode = wd->parents->parent->id;
					}
					fsnodes_fill_attr(wd->parents->parent,wd,uid,gid,auid,agid,sesflags,attr);
				} else {
					*inode=MFS_ROOT_ID; // rn->id;
					fsnodes_fill_attr(rn,wd,uid,gid,auid,agid,sesflags,attr);
				}
			}
			stats_lookup++;
			return STATUS_OK;
		}
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return ERROR_EINVAL;
	}
	e = fsnodes_lookup(wd,nleng,name);
	if (!e) {
		return ERROR_ENOENT;
	}
/*
#ifdef CACHENOTIFY
	fsnodes_attr_access(e->child,ts);
#endif
*/
	*inode = e->child->id;
	fsnodes_fill_attr(e->child,wd,uid,gid,auid,agid,sesflags,attr);
	stats_lookup++;
	return STATUS_OK;
}

uint8_t fs_getattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t attr[35]) {
	fsnode *p,*rn;
/*
#ifdef CACHENOTIFY
	uint32_t ts = main_time();
#endif
*/

	(void)sesflags;
	memset(attr,0,35);
	if (rootinode==MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
/*
#ifdef CACHENOTIFY
	fsnodes_attr_access(p,ts);
#endif
*/
	fsnodes_fill_attr(p,NULL,uid,gid,auid,agid,sesflags,attr);
	stats_getattr++;
	return STATUS_OK;
}

uint8_t fs_try_setlength(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint64_t length,uint8_t attr[35],uint64_t *chunkid) {
	fsnode *p,*rn;
	memset(attr,0,35);
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (rootinode==MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	if (opened==0) {
		if (!fsnodes_access(p,uid,gid,MODE_MASK_W,sesflags)) {
			return ERROR_EACCES;
		}
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	if (length&MFSCHUNKMASK) {
		uint32_t indx = (length>>MFSCHUNKBITS);
		if (indx<p->data.fdata.chunks) {
			uint64_t ochunkid = p->data.fdata.chunktab[indx];
			if (ochunkid>0) {
				uint8_t status;
				uint64_t nchunkid;
				bool truncatingUpwards = (length >= p->data.fdata.length);
				if (!truncatingUpwards) {
					syslog(LOG_WARNING,
							"Potentially dangerous (not fully supported) truncate downwards - "
							"inode %" PRIu32 " old length: %" PRIu64 " ; new length: %" PRIu64 ")",
							inode, p->data.fdata.length, length);
				}
				status = chunk_multi_truncate(&nchunkid, ochunkid, length & MFSCHUNKMASK, p->goal,
						truncatingUpwards, fsnodes_size_quota_exceeded(p->uid, p->gid));
				if (status!=STATUS_OK) {
					return status;
				}
				p->data.fdata.chunktab[indx] = nchunkid;
				*chunkid = nchunkid;
				changelog(metaversion++,"%" PRIu32 "|TRUNC(%" PRIu32 ",%" PRIu32 "):%" PRIu64,(uint32_t)main_time(),inode,indx,nchunkid);
				fsnodes_update_checksum(p);
				return ERROR_DELAYED;
			}
		}
	}
	fsnodes_fill_attr(p,NULL,uid,gid,auid,agid,sesflags,attr);
	stats_setattr++;
	return STATUS_OK;
}
#endif

#ifdef METARESTORE
uint8_t fs_trunc(uint32_t ts,uint32_t inode,uint32_t indx,uint64_t chunkid) {
	uint64_t ochunkid,nchunkid;
	uint8_t status;
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EINVAL;
	}
	if (indx>MAX_INDEX) {
		return ERROR_INDEXTOOBIG;
	}
	if (indx>=p->data.fdata.chunks) {
		return ERROR_EINVAL;
	}
	ochunkid = p->data.fdata.chunktab[indx];
	status = chunk_multi_truncate(ts,&nchunkid,ochunkid,p->goal);
	if (status!=STATUS_OK) {
		return status;
	}
	if (chunkid!=nchunkid) {
		return ERROR_MISMATCH;
	}
	p->data.fdata.chunktab[indx] = nchunkid;
	metaversion++;
	fsnodes_update_checksum(p);
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint8_t fs_end_setlength(uint64_t chunkid) {
	changelog(metaversion++,"%" PRIu32 "|UNLOCK(%" PRIu64 ")",(uint32_t)main_time(),chunkid);
	return chunk_unlock(chunkid);
}
#else
uint8_t fs_unlock(uint64_t chunkid) {
	metaversion++;
	return chunk_unlock(chunkid);
}
#endif

#ifndef METARESTORE
uint8_t fs_do_setlength(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint64_t length,uint8_t attr[35]) {
	fsnode *p,*rn;
	uint32_t ts = main_time();

	memset(attr,0,35);
	if (rootinode==MFS_ROOT_ID || rootinode==0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
		if (rootinode==0 && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
			return ERROR_EPERM;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	fsnodes_setlength(p,length);
	changelog(metaversion++,"%" PRIu32 "|LENGTH(%" PRIu32 ",%" PRIu64 ")",ts,inode,p->data.fdata.length);
	p->ctime = p->mtime = ts;
	fsnodes_update_checksum(p);
	fsnodes_fill_attr(p,NULL,uid,gid,auid,agid,sesflags,attr);
/*
#ifdef CACHENOTIFY
	fsnodes_attr_changed(p,ts);
#endif
*/
	stats_setattr++;
	return STATUS_OK;
}


uint8_t fs_setattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t setmask,uint16_t attrmode,uint32_t attruid,uint32_t attrgid,uint32_t attratime,uint32_t attrmtime,uint8_t sugidclearmode,uint8_t attr[35]) {
	fsnode *p,*rn;
	uint32_t ts = main_time();

	memset(attr,0,35);
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (rootinode==MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	if (uid!=0 && (sesflags&SESFLAG_MAPALL) && (setmask&(SET_UID_FLAG|SET_GID_FLAG))) {
		return ERROR_EPERM;
	}
	if ((p->mode&(EATTR_NOOWNER<<12))==0 && uid!=0 && uid!=p->uid) {
		if (setmask & (SET_MODE_FLAG | SET_UID_FLAG | SET_GID_FLAG)) {
			return ERROR_EPERM;
		}
		if ((setmask & SET_ATIME_FLAG) && !(setmask & SET_ATIME_NOW_FLAG)) {
			return ERROR_EPERM;
		}
		if ((setmask & SET_MTIME_FLAG) && !(setmask & SET_MTIME_NOW_FLAG)) {
			return ERROR_EPERM;
		}
		if ((setmask & (SET_ATIME_NOW_FLAG | SET_MTIME_NOW_FLAG))
				&& !fsnodes_access(p, uid, gid, MODE_MASK_W, sesflags)) {
			return ERROR_EACCES;
		}
	}
	if (uid!=0 && uid!=attruid && (setmask&SET_UID_FLAG)) {
		return ERROR_EPERM;
	}
	if ((sesflags&SESFLAG_IGNOREGID)==0) {
		if (uid!=0 && gid!=attrgid && (setmask&SET_GID_FLAG)) {
			return ERROR_EPERM;
		}
	}
	// first ignore sugid clears done by kernel
	if ((setmask&(SET_UID_FLAG|SET_GID_FLAG)) && (setmask&SET_MODE_FLAG)) { // chown+chmod = chown with sugid clears
		attrmode |= (p->mode & 06000);
	}
	// then do it yourself
	if ((p->mode & 06000) && (setmask&(SET_UID_FLAG|SET_GID_FLAG))) { // this is "chown" operation and suid or sgid bit is set
		switch (sugidclearmode) {
		case SUGID_CLEAR_MODE_ALWAYS:
			p->mode &= 0171777; // safest approach - always delete both suid and sgid
			attrmode &= 01777;
			break;
		case SUGID_CLEAR_MODE_OSX:
			if (uid!=0) { // OSX+Solaris - every change done by unprivileged user should clear suid and sgid
				p->mode &= 0171777;
				attrmode &= 01777;
			}
			break;
		case SUGID_CLEAR_MODE_BSD:
			if (uid!=0 && (setmask&SET_GID_FLAG) && p->gid!=attrgid) { // *BSD - like in OSX but only when something is actually changed
				p->mode &= 0171777;
				attrmode &= 01777;
			}
			break;
		case SUGID_CLEAR_MODE_EXT:
			if (p->type!=TYPE_DIRECTORY) {
				if (p->mode & 010) { // when group exec is set - clear both bits
					p->mode &= 0171777;
					attrmode &= 01777;
				} else { // when group exec is not set - clear suid only
					p->mode &= 0173777;
					attrmode &= 03777;
				}
			}
			break;
		case SUGID_CLEAR_MODE_XFS:
			if (p->type!=TYPE_DIRECTORY) { // similar to EXT3, but unprivileged users also clear suid/sgid bits on directories
				if (p->mode & 010) {
					p->mode &= 0171777;
					attrmode &= 01777;
				} else {
					p->mode &= 0173777;
					attrmode &= 03777;
				}
			} else if (uid!=0) {
				p->mode &= 0171777;
				attrmode &= 01777;
			}
			break;
		}
	}
	if (setmask&SET_MODE_FLAG) {
		p->mode = (attrmode & 07777) | (p->mode & 0xF000);
	}
	if (setmask & (SET_UID_FLAG | SET_GID_FLAG)) {
		int64_t size;
		fsnodes_quota_unregister_inode(p);
		if (p->type == TYPE_FILE || p->type == TYPE_TRASH || p->type == TYPE_RESERVED) {
			size = fsnodes_get_size(p);
			fsnodes_quota_update_size(p, -size);
		}
		if (setmask&SET_UID_FLAG) {
			p->uid = attruid;
		}
		if (setmask&SET_GID_FLAG) {
			p->gid = attrgid;
		}
		fsnodes_quota_register_inode(p);
		if (p->type == TYPE_FILE || p->type == TYPE_TRASH || p->type == TYPE_RESERVED) {
			fsnodes_quota_update_size(p, +size);
		}
	}
	if (setmask&SET_ATIME_NOW_FLAG) {
		p->atime = ts;
	} else if (setmask&SET_ATIME_FLAG) {
		p->atime = attratime;
	}
	if (setmask&SET_MTIME_NOW_FLAG) {
		p->mtime = ts;
	} else if (setmask&SET_MTIME_FLAG) {
		p->mtime = attrmtime;
	}
	changelog(metaversion++,"%" PRIu32 "|ATTR(%" PRIu32 ",%" PRIu16",%" PRIu32 ",%" PRIu32 ",%" PRIu32 ",%" PRIu32 ")",ts,inode,p->mode & 07777,p->uid,p->gid,p->atime,p->mtime);
	p->ctime = ts;
	fsnodes_fill_attr(p,NULL,uid,gid,auid,agid,sesflags,attr);
	fsnodes_update_checksum(p);
/*
#ifdef CACHENOTIFY
	fsnodes_attr_changed(p,ts);
#endif
*/
	stats_setattr++;
	return STATUS_OK;
}
#endif


#ifdef METARESTORE
uint8_t fs_attr(uint32_t ts,uint32_t inode,uint32_t mode,uint32_t uid,uint32_t gid,uint32_t atime,uint32_t mtime) {
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (mode>07777) {
		return ERROR_EINVAL;
	}
	p->mode = mode | (p->mode & 0xF000);
	p->uid = uid;
	p->gid = gid;
	p->atime = atime;
	p->mtime = mtime;
	p->ctime = ts;
	fsnodes_update_checksum(p);
	metaversion++;
	return STATUS_OK;
}

uint8_t fs_length(uint32_t ts,uint32_t inode,uint64_t length) {
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EINVAL;
	}
	fsnodes_setlength(p,length);
	p->mtime = ts;
	p->ctime = ts;
	fsnodes_update_checksum(p);
	metaversion++;
	return STATUS_OK;
}

#endif

#ifndef METARESTORE
uint8_t fs_readlink(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t *pleng,uint8_t **path) {
	fsnode *p,*rn;
	uint32_t ts = main_time();

	(void)sesflags;
	*pleng = 0;
	*path = NULL;
	if (rootinode==MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	if (p->type!=TYPE_SYMLINK) {
		return ERROR_EINVAL;
	}
	*pleng = p->data.sdata.pleng;
	*path = p->data.sdata.path;
	if (p->atime!=ts) {
		p->atime = ts;
		fsnodes_update_checksum(p);
/*
#ifdef CACHENOTIFY
		fsnodes_attr_changed(p,ts);
#endif
*/
		changelog(metaversion++,"%" PRIu32 "|ACCESS(%" PRIu32 ")",ts,inode);
	}
	stats_readlink++;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint8_t fs_symlink(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t pleng,const uint8_t *path,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[35]) {
#else
uint8_t fs_symlink(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,const uint8_t *path,uint32_t uid,uint32_t gid,uint32_t inode) {
	uint32_t pleng;
#endif
	fsnode *wd,*p;
	uint8_t *newpath;
#ifndef METARESTORE
	fsnode *rn;
	statsrecord sr;
	uint32_t i;
	*inode = 0;
	memset(attr,0,35);
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (pleng==0) {
		return ERROR_EINVAL;
	}
	for (i=0 ; i<pleng ; i++) {
		if (path[i]==0) {
			return ERROR_EINVAL;
		}
	}
	if (rootinode==MFS_ROOT_ID) {
		wd = fsnodes_id_to_node(parent);
		if (!wd) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (parent==MFS_ROOT_ID) {
			parent = rootinode;
			wd = rn;
		} else {
			wd = fsnodes_id_to_node(parent);
			if (!wd) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,wd)) {
				return ERROR_EPERM;
			}
		}
	}
#else
	pleng = strlen((const char*)path);
	wd = fsnodes_id_to_node(parent);
	if (!wd) {
		return ERROR_ENOENT;
	}
#endif
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
#ifndef METARESTORE
	if (!fsnodes_access(wd,uid,gid,MODE_MASK_W,sesflags)) {
		return ERROR_EACCES;
	}
#endif
	if (fsnodes_namecheck(nleng,name)<0) {
		return ERROR_EINVAL;
	}
	if (fsnodes_nameisused(wd,nleng,name)) {
		return ERROR_EEXIST;
	}
#ifndef METARESTORE
	if (fsnodes_inode_quota_exceeded(uid, gid)) {
		return ERROR_QUOTA;
	}
#endif
	newpath = (uint8_t*) malloc(pleng);
	passert(newpath);
#ifndef METARESTORE
	p = fsnodes_create_node(main_time(),wd,nleng,name,TYPE_SYMLINK,0777,0,uid,gid,0,AclInheritance::kDontInheritAcl);
#else
	p = fsnodes_create_node(ts,wd,nleng,name,TYPE_SYMLINK,0777,0,uid,gid,0,AclInheritance::kDontInheritAcl);
#endif
	memcpy(newpath,path,pleng);
	p->data.sdata.path = newpath;
	p->data.sdata.pleng = pleng;
	fsnodes_update_checksum(p);
#ifndef METARESTORE

	memset(&sr,0,sizeof(statsrecord));
	sr.length = pleng;
	fsnodes_add_stats(wd,&sr);

	*inode = p->id;
	fsnodes_fill_attr(p,wd,uid,gid,auid,agid,sesflags,attr);
	changelog(metaversion++,"%" PRIu32 "|SYMLINK(%" PRIu32 ",%s,%s,%" PRIu32 ",%" PRIu32 "):%" PRIu32,(uint32_t)main_time(),parent,fsnodes_escape_name(nleng,name),fsnodes_escape_name(pleng,newpath),uid,gid,p->id);
	stats_symlink++;
#else
	if (inode!=p->id) {
		return ERROR_MISMATCH;
	}
	metaversion++;
#endif
	return STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_mknod(uint32_t rootinode, uint8_t sesflags, uint32_t parent, uint16_t nleng,
		const uint8_t *name, uint8_t type, uint16_t mode, uint16_t umask, uint32_t uid,
		uint32_t gid, uint32_t auid, uint32_t agid, uint32_t rdev,
		uint32_t *inode, uint8_t attr[35]) {
	fsnode *wd,*p,*rn;
	*inode = 0;
	memset(attr,0,35);
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (type!=TYPE_FILE && type!=TYPE_SOCKET && type!=TYPE_FIFO && type!=TYPE_BLOCKDEV && type!=TYPE_CHARDEV) {
		return ERROR_EINVAL;
	}
	if (rootinode==MFS_ROOT_ID) {
		wd = fsnodes_id_to_node(parent);
		if (!wd) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (parent==MFS_ROOT_ID) {
			parent = rootinode;
			wd = rn;
		} else {
			wd = fsnodes_id_to_node(parent);
			if (!wd) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,wd)) {
				return ERROR_EPERM;
			}
		}
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if (!fsnodes_access(wd,uid,gid,MODE_MASK_W,sesflags)) {
		return ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return ERROR_EINVAL;
	}
	if (fsnodes_nameisused(wd,nleng,name)) {
		return ERROR_EEXIST;
	}
	if (fsnodes_inode_quota_exceeded(uid, gid)) {
		return ERROR_QUOTA;
	}
	p = fsnodes_create_node(main_time(),wd,nleng,name,type,mode,umask,uid,gid,0,AclInheritance::kInheritAcl);
	if (type==TYPE_BLOCKDEV || type==TYPE_CHARDEV) {
		p->data.devdata.rdev = rdev;
	}
	*inode = p->id;
	fsnodes_fill_attr(p,wd,uid,gid,auid,agid,sesflags,attr);
	changelog(metaversion++,"%" PRIu32 "|CREATE(%" PRIu32 ",%s,%c,%" PRIu16",%" PRIu32 ",%" PRIu32 ",%" PRIu32 "):%" PRIu32,(uint32_t)main_time(),parent,fsnodes_escape_name(nleng,name),type,p->mode & 07777,uid,gid,rdev,p->id);
	stats_mknod++;
	fsnodes_update_checksum(p);
	return STATUS_OK;
}

uint8_t fs_mkdir(uint32_t rootinode, uint8_t sesflags, uint32_t parent, uint16_t nleng,
		const uint8_t *name, uint16_t mode, uint16_t umask, uint32_t uid, uint32_t gid,
		uint32_t auid, uint32_t agid, uint8_t copysgid, uint32_t *inode, uint8_t attr[35]) {
	fsnode *wd,*p,*rn;
	*inode = 0;
	memset(attr,0,35);
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (rootinode==MFS_ROOT_ID) {
		wd = fsnodes_id_to_node(parent);
		if (!wd) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (parent==MFS_ROOT_ID) {
			parent = rootinode;
			wd = rn;
		} else {
			wd = fsnodes_id_to_node(parent);
			if (!wd) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,wd)) {
				return ERROR_EPERM;
			}
		}
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if (!fsnodes_access(wd,uid,gid,MODE_MASK_W,sesflags)) {
		return ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return ERROR_EINVAL;
	}
	if (fsnodes_nameisused(wd,nleng,name)) {
		return ERROR_EEXIST;
	}
	if (fsnodes_inode_quota_exceeded(uid, gid)) {
		return ERROR_QUOTA;
	}
	p = fsnodes_create_node(main_time(),wd,nleng,name,TYPE_DIRECTORY,mode,umask,uid,gid,copysgid,AclInheritance::kInheritAcl);
	*inode = p->id;
	fsnodes_fill_attr(p,wd,uid,gid,auid,agid,sesflags,attr);
	changelog(metaversion++,"%" PRIu32 "|CREATE(%" PRIu32 ",%s,%c,%" PRIu16",%" PRIu32 ",%" PRIu32 ",%" PRIu32 "):%" PRIu32,(uint32_t)main_time(),parent,fsnodes_escape_name(nleng,name),TYPE_DIRECTORY,p->mode & 07777,uid,gid,0,p->id);
	stats_mkdir++;
	return STATUS_OK;
}
#else
uint8_t fs_create(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,uint8_t type,uint32_t mode,uint32_t uid,uint32_t gid,uint32_t rdev,uint32_t inode) {
	fsnode *wd,*p;
	if (type!=TYPE_FILE && type!=TYPE_SOCKET && type!=TYPE_FIFO && type!=TYPE_BLOCKDEV && type!=TYPE_CHARDEV && type!=TYPE_DIRECTORY) {
		return ERROR_EINVAL;
	}
	wd = fsnodes_id_to_node(parent);
	if (!wd) {
		return ERROR_ENOENT;
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if (fsnodes_nameisused(wd,nleng,name)) {
		return ERROR_EEXIST;
	}
	p = fsnodes_create_node(ts,wd,nleng,name,type,mode,0,uid,gid,0,AclInheritance::kDontInheritAcl);
	if (type==TYPE_BLOCKDEV || type==TYPE_CHARDEV) {
		p->data.devdata.rdev = rdev;
		fsnodes_update_checksum(p);
	}
	if (inode!=p->id) {
		return ERROR_MISMATCH;
	}
	metaversion++;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint8_t fs_unlink(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid) {
	uint32_t ts;
	fsnode *wd,*rn;
	fsedge *e;
	ts = main_time();
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (rootinode==MFS_ROOT_ID) {
		wd = fsnodes_id_to_node(parent);
		if (!wd) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (parent==MFS_ROOT_ID) {
			parent = rootinode;
			wd = rn;
		} else {
			wd = fsnodes_id_to_node(parent);
			if (!wd) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,wd)) {
				return ERROR_EPERM;
			}
		}
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if (!fsnodes_access(wd,uid,gid,MODE_MASK_W,sesflags)) {
		return ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return ERROR_EINVAL;
	}
	e = fsnodes_lookup(wd,nleng,name);
	if (!e) {
		return ERROR_ENOENT;
	}
	if (!fsnodes_sticky_access(wd,e->child,uid)) {
		return ERROR_EPERM;
	}
	if (e->child->type==TYPE_DIRECTORY) {
		return ERROR_EPERM;
	}
	changelog(metaversion++,"%" PRIu32 "|UNLINK(%" PRIu32 ",%s):%" PRIu32,ts,parent,fsnodes_escape_name(nleng,name),e->child->id);
	fsnodes_unlink(ts,e);
	stats_unlink++;
	return STATUS_OK;
}

uint8_t fs_rmdir(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid) {
	uint32_t ts;
	fsnode *wd,*rn;
	fsedge *e;
	ts = main_time();
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (rootinode==MFS_ROOT_ID) {
		wd = fsnodes_id_to_node(parent);
		if (!wd) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (parent==MFS_ROOT_ID) {
			parent = rootinode;
			wd = rn;
		} else {
			wd = fsnodes_id_to_node(parent);
			if (!wd) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,wd)) {
				return ERROR_EPERM;
			}
		}
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if (!fsnodes_access(wd,uid,gid,MODE_MASK_W,sesflags)) {
		return ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return ERROR_EINVAL;
	}
	e = fsnodes_lookup(wd,nleng,name);
	if (!e) {
		return ERROR_ENOENT;
	}
	if (!fsnodes_sticky_access(wd,e->child,uid)) {
		return ERROR_EPERM;
	}
	if (e->child->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if (e->child->data.ddata.children!=NULL) {
		return ERROR_ENOTEMPTY;
	}
	changelog(metaversion++,"%" PRIu32 "|UNLINK(%" PRIu32 ",%s):%" PRIu32,ts,parent,fsnodes_escape_name(nleng,name),e->child->id);
	fsnodes_unlink(ts,e);
	stats_rmdir++;
	return STATUS_OK;
}
#else
uint8_t fs_unlink(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,uint32_t inode) {
	fsnode *wd;
	fsedge *e;
	wd = fsnodes_id_to_node(parent);
	if (!wd) {
		return ERROR_ENOENT;
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	e = fsnodes_lookup(wd,nleng,name);
	if (!e) {
		return ERROR_ENOENT;
	}
	if (e->child->id!=inode) {
		return ERROR_MISMATCH;
	}
	if (e->child->type==TYPE_DIRECTORY && e->child->data.ddata.children!=NULL) {
		return ERROR_ENOTEMPTY;
	}
	fsnodes_unlink(ts,e);
	metaversion++;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint8_t fs_rename(uint32_t rootinode,uint8_t sesflags,uint32_t parent_src,uint16_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[35]) {
	uint32_t ts;
#else
uint8_t fs_move(uint32_t ts,uint32_t parent_src,uint32_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint32_t nleng_dst,const uint8_t *name_dst,uint32_t inode) {
#endif
	fsnode *swd;
	fsedge *se;
	fsnode *dwd;
	fsedge *de;
	fsnode *node;
#ifndef METARESTORE
	fsnode *rn;
	ts = main_time();
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (rootinode==MFS_ROOT_ID) {
		swd = fsnodes_id_to_node(parent_src);
		if (!swd) {
			return ERROR_ENOENT;
		}
		dwd = fsnodes_id_to_node(parent_dst);
		if (!dwd) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (parent_src==MFS_ROOT_ID) {
			parent_src = rootinode;
			swd = rn;
		} else {
			swd = fsnodes_id_to_node(parent_src);
			if (!swd) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,swd)) {
				return ERROR_EPERM;
			}
		}
		if (parent_dst==MFS_ROOT_ID) {
			parent_dst = rootinode;
			dwd = rn;
		} else {
			dwd = fsnodes_id_to_node(parent_dst);
			if (!dwd) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,dwd)) {
				return ERROR_EPERM;
			}
		}
	}
#else
	swd = fsnodes_id_to_node(parent_src);
	if (!swd) {
		return ERROR_ENOENT;
	}
	dwd = fsnodes_id_to_node(parent_dst);
	if (!dwd) {
		return ERROR_ENOENT;
	}
#endif
	if (swd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
#ifndef METARESTORE
	if (!fsnodes_access(swd,uid,gid,MODE_MASK_W,sesflags)) {
		return ERROR_EACCES;
	}
#endif
	if (fsnodes_namecheck(nleng_src,name_src)<0) {
		return ERROR_EINVAL;
	}
	se = fsnodes_lookup(swd,nleng_src,name_src);
	if (!se) {
		return ERROR_ENOENT;
	}
	node = se->child;
#ifndef METARESTORE
	if (!fsnodes_sticky_access(swd,node,uid)) {
		return ERROR_EPERM;
	}
#endif
#ifdef METARESTORE
	if (node->id!=inode) {
		return ERROR_MISMATCH;
	}
#endif
	if (dwd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
#ifndef METARESTORE
	if (!fsnodes_access(dwd,uid,gid,MODE_MASK_W,sesflags)) {
		return ERROR_EACCES;
	}
#endif
	if (se->child->type==TYPE_DIRECTORY) {
		if (fsnodes_isancestor(se->child,dwd)) {
			return ERROR_EINVAL;
		}
	}
	if (fsnodes_namecheck(nleng_dst,name_dst)<0) {
		return ERROR_EINVAL;
	}
	de = fsnodes_lookup(dwd,nleng_dst,name_dst);
	if (de) {
		if (de->child->type==TYPE_DIRECTORY && de->child->data.ddata.children!=NULL) {
			return ERROR_ENOTEMPTY;
		}
#ifndef METARESTORE
		if (!fsnodes_sticky_access(dwd,de->child,uid)) {
			return ERROR_EPERM;
		}
#endif
		fsnodes_unlink(ts,de);
	}
	fsnodes_remove_edge(ts,se);
	fsnodes_link(ts,dwd,node,nleng_dst,name_dst);
#ifndef METARESTORE
	*inode = node->id;
	fsnodes_fill_attr(node,dwd,uid,gid,auid,agid,sesflags,attr);
	changelog(metaversion++,"%" PRIu32 "|MOVE(%" PRIu32 ",%s,%" PRIu32 ",%s):%" PRIu32,(uint32_t)main_time(),parent_src,fsnodes_escape_name(nleng_src,name_src),parent_dst,fsnodes_escape_name(nleng_dst,name_dst),node->id);
	stats_rename++;
#else
	metaversion++;
#endif
	return STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_link(uint32_t rootinode,uint8_t sesflags,uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[35]) {
	uint32_t ts;
#else
uint8_t fs_link(uint32_t ts,uint32_t inode_src,uint32_t parent_dst,uint32_t nleng_dst,uint8_t *name_dst) {
#endif
	fsnode *sp;
	fsnode *dwd;
#ifndef METARESTORE
	fsnode *rn;
	ts = main_time();
	*inode = 0;
	memset(attr,0,35);
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (rootinode==MFS_ROOT_ID) {
		sp = fsnodes_id_to_node(inode_src);
		if (!sp) {
			return ERROR_ENOENT;
		}
		dwd = fsnodes_id_to_node(parent_dst);
		if (!dwd) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode_src==MFS_ROOT_ID) {
			inode_src = rootinode;
			sp = rn;
		} else {
			sp = fsnodes_id_to_node(inode_src);
			if (!sp) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,sp)) {
				return ERROR_EPERM;
			}
		}
		if (parent_dst==MFS_ROOT_ID) {
			parent_dst = rootinode;
			dwd = rn;
		} else {
			dwd = fsnodes_id_to_node(parent_dst);
			if (!dwd) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,dwd)) {
				return ERROR_EPERM;
			}
		}
	}
#else
	sp = fsnodes_id_to_node(inode_src);
	if (!sp) {
		return ERROR_ENOENT;
	}
	dwd = fsnodes_id_to_node(parent_dst);
	if (!dwd) {
		return ERROR_ENOENT;
	}
#endif
	if (sp->type==TYPE_TRASH || sp->type==TYPE_RESERVED) {
		return ERROR_ENOENT;
	}
	if (sp->type==TYPE_DIRECTORY) {
		return ERROR_EPERM;
	}
	if (dwd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
#ifndef METARESTORE
	if (!fsnodes_access(dwd,uid,gid,MODE_MASK_W,sesflags)) {
		return ERROR_EACCES;
	}
#endif
	if (fsnodes_namecheck(nleng_dst,name_dst)<0) {
		return ERROR_EINVAL;
	}
	if (fsnodes_nameisused(dwd,nleng_dst,name_dst)) {
		return ERROR_EEXIST;
	}
	fsnodes_link(ts,dwd,sp,nleng_dst,name_dst);
#ifndef METARESTORE
	*inode = inode_src;
	fsnodes_fill_attr(sp,dwd,uid,gid,auid,agid,sesflags,attr);
	changelog(metaversion++,"%" PRIu32 "|LINK(%" PRIu32 ",%" PRIu32 ",%s)",(uint32_t)main_time(),inode_src,parent_dst,fsnodes_escape_name(nleng_dst,name_dst));
	stats_link++;
#else
	metaversion++;
#endif
	return STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_snapshot(uint32_t rootinode,uint8_t sesflags,uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gid,uint8_t canoverwrite) {
	uint32_t ts;
	fsnode *rn;
#else
uint8_t fs_snapshot(uint32_t ts,uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,uint8_t *name_dst,uint8_t canoverwrite) {
#endif
	fsnode *sp;
	fsnode *dwd;
	uint8_t status;
#ifndef METARESTORE
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (rootinode==MFS_ROOT_ID) {
		sp = fsnodes_id_to_node(inode_src);
		if (!sp) {
			return ERROR_ENOENT;
		}
		dwd = fsnodes_id_to_node(parent_dst);
		if (!dwd) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode_src==MFS_ROOT_ID) {
			inode_src = rootinode;
			sp = rn;
		} else {
			sp = fsnodes_id_to_node(inode_src);
			if (!sp) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,sp)) {
				return ERROR_EPERM;
			}
		}
		if (parent_dst==MFS_ROOT_ID) {
			parent_dst = rootinode;
			dwd = rn;
		} else {
			dwd = fsnodes_id_to_node(parent_dst);
			if (!dwd) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,dwd)) {
				return ERROR_EPERM;
			}
		}
	}
#else
	sp = fsnodes_id_to_node(inode_src);
	if (!sp) {
		return ERROR_ENOENT;
	}
	dwd = fsnodes_id_to_node(parent_dst);
	if (!dwd) {
		return ERROR_ENOENT;
	}
#endif
#ifndef METARESTORE
	if (!fsnodes_access(sp,uid,gid,MODE_MASK_R,sesflags)) {
		return ERROR_EACCES;
	}
#endif
	if (dwd->type!=TYPE_DIRECTORY) {
		return ERROR_EPERM;
	}
	if (sp->type==TYPE_DIRECTORY) {
		if (sp==dwd || fsnodes_isancestor(sp,dwd)) {
			return ERROR_EINVAL;
		}
	}
#ifndef METARESTORE
	if (!fsnodes_access(dwd,uid,gid,MODE_MASK_W,sesflags)) {
		return ERROR_EACCES;
	}
#endif
	status = fsnodes_snapshot_test(sp,sp,dwd,nleng_dst,name_dst,canoverwrite);
	if (status!=STATUS_OK) {
		return status;
	}
#ifndef METARESTORE
	ts = main_time();
#endif
	fsnodes_snapshot(ts,sp,dwd,nleng_dst,name_dst);
#ifndef METARESTORE
	changelog(metaversion++,"%" PRIu32 "|SNAPSHOT(%" PRIu32 ",%" PRIu32 ",%s,%" PRIu8 ")",ts,inode_src,parent_dst,fsnodes_escape_name(nleng_dst,name_dst),canoverwrite);
#else
	metaversion++;
#endif
	return STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_append(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t inode_src,uint32_t uid,uint32_t gid) {
	uint32_t ts;
	fsnode *rn;
#else
uint8_t fs_append(uint32_t ts,uint32_t inode,uint32_t inode_src) {
#endif
	uint8_t status;
	fsnode *p,*sp;
	if (inode==inode_src) {
		return ERROR_EINVAL;
	}
#ifndef METARESTORE
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (rootinode==MFS_ROOT_ID) {
		sp = fsnodes_id_to_node(inode_src);
		if (!sp) {
			return ERROR_ENOENT;
		}
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode_src==MFS_ROOT_ID) {
			inode_src = rootinode;
			sp = rn;
		} else {
			sp = fsnodes_id_to_node(inode_src);
			if (!sp) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,sp)) {
				return ERROR_EPERM;
			}
		}
		if (inode==MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
#else
	sp = fsnodes_id_to_node(inode_src);
	if (!sp) {
		return ERROR_ENOENT;
	}
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
#endif
	if (sp->type!=TYPE_FILE && sp->type!=TYPE_TRASH && sp->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
#ifndef METARESTORE
	if (!fsnodes_access(sp,uid,gid,MODE_MASK_R,sesflags)) {
		return ERROR_EACCES;
	}
#endif
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
#ifndef METARESTORE
	if (!fsnodes_access(p,uid,gid,MODE_MASK_W,sesflags)) {
		return ERROR_EACCES;
	}
	if (fsnodes_size_quota_exceeded(p->uid, p->gid)) {
		return ERROR_QUOTA;
	}
	ts = main_time();
#endif
	status = fsnodes_appendchunks(ts,p,sp);
	if (status!=STATUS_OK) {
		return status;
	}
#ifndef METARESTORE
	changelog(metaversion++,"%" PRIu32 "|APPEND(%" PRIu32 ",%" PRIu32 ")",ts,inode,inode_src);
#else
	metaversion++;
#endif
	return STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_readdir_size(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint8_t flags,void **dnode,uint32_t *dbuffsize) {
	fsnode *p,*rn;
	*dnode = NULL;
	*dbuffsize = 0;
	if (rootinode==MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	if (p->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if (!fsnodes_access(p,uid,gid,MODE_MASK_R,sesflags)) {
		return ERROR_EACCES;
	}
	*dnode = p;
	*dbuffsize = fsnodes_getdirsize(p,flags&GETDIR_FLAG_WITHATTR);
	return STATUS_OK;
}

void fs_readdir_data(uint32_t rootinode,uint8_t sesflags,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t flags,void *dnode,uint8_t *dbuff) {
	fsnode *p = (fsnode*)dnode;
	uint32_t ts = main_time();

	if (p->atime!=ts) {
		p->atime = ts;
		fsnodes_update_checksum(p);
		changelog(metaversion++,"%" PRIu32 "|ACCESS(%" PRIu32 ")",ts,p->id);
		fsnodes_getdirdata(rootinode,uid,gid,auid,agid,sesflags,p,dbuff,flags&GETDIR_FLAG_WITHATTR);
/*
#ifdef CACHENOTIFY
		fsnodes_attr_changed(p,ts);
#endif
*/
	} else {
		fsnodes_getdirdata(rootinode,uid,gid,auid,agid,sesflags,p,dbuff,flags&GETDIR_FLAG_WITHATTR);
	}
	stats_readdir++;
}


uint8_t fs_checkfile(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t chunkcount[11]) {
	fsnode *p,*rn;
	(void)sesflags;
	if (rootinode==MFS_ROOT_ID || rootinode==0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
		if (rootinode==0 && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
			return ERROR_EPERM;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	fsnodes_checkfile(p,chunkcount);
	return STATUS_OK;
}

uint8_t fs_opencheck(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t flags,uint8_t attr[35]) {
	fsnode *p,*rn;
	if ((sesflags&SESFLAG_READONLY) && (flags&WANT_WRITE)) {
		return ERROR_EROFS;
	}
	if (rootinode==MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	if ((flags&AFTER_CREATE)==0) {
		uint8_t modemask=0;
		if (flags&WANT_READ) {
			modemask|=MODE_MASK_R;
		}
		if (flags&WANT_WRITE) {
			modemask|=MODE_MASK_W;
		}
		if (!fsnodes_access(p,uid,gid,modemask,sesflags)) {
			return ERROR_EACCES;
		}
	}
	fsnodes_fill_attr(p,NULL,uid,gid,auid,agid,sesflags,attr);
	stats_open++;
	return STATUS_OK;
}
#endif


uint8_t fs_acquire(uint32_t inode,uint32_t sessionid) {
	fsnode *p;
	sessionidrec *cr;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	for (cr=p->data.fdata.sessionids ; cr ; cr=cr->next) {
		if (cr->sessionid==sessionid) {
			return ERROR_EINVAL;
		}
	}
	cr = sessionidrec_malloc();
	cr->sessionid = sessionid;
	cr->next = p->data.fdata.sessionids;
	p->data.fdata.sessionids = cr;
	fsnodes_update_checksum(p);
#ifndef METARESTORE
	changelog(metaversion++,"%" PRIu32 "|ACQUIRE(%" PRIu32 ",%" PRIu32 ")",(uint32_t)main_time(),inode,sessionid);
#else
	metaversion++;
#endif
	return STATUS_OK;
}

uint8_t fs_release(uint32_t inode,uint32_t sessionid) {
	fsnode *p;
	sessionidrec *cr,**crp;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	crp = &(p->data.fdata.sessionids);
	while ((cr=*crp)) {
		if (cr->sessionid==sessionid) {
			*crp = cr->next;
			sessionidrec_free(cr);
#ifndef METARESTORE
			changelog(metaversion++,"%" PRIu32 "|RELEASE(%" PRIu32 ",%" PRIu32 ")",(uint32_t)main_time(),inode,sessionid);
#else
			metaversion++;
#endif
			fsnodes_update_checksum(p);
			return STATUS_OK;
		} else {
			crp = &(cr->next);
		}
	}
#ifndef METARESTORE
	syslog(LOG_WARNING,"release: session not found");
#endif
	return ERROR_EINVAL;
}

#ifndef METARESTORE
uint32_t fs_newsessionid(void) {
	changelog(metaversion++,"%" PRIu32 "|SESSION():%" PRIu32,(uint32_t)main_time(),nextsessionid);
	return nextsessionid++;
}
#else
uint8_t fs_session(uint32_t sessionid) {
	if (sessionid!=nextsessionid) {
		return ERROR_MISMATCH;
	}
	metaversion++;
	nextsessionid++;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint64_t *chunkid,uint64_t *length) {
	fsnode *p;
	uint32_t ts = main_time();

	*chunkid = 0;
	*length = 0;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	if (indx>MAX_INDEX) {
		return ERROR_INDEXTOOBIG;
	}
	if (indx<p->data.fdata.chunks) {
		*chunkid = p->data.fdata.chunktab[indx];
	}
	*length = p->data.fdata.length;
	if (p->atime!=ts) {
		p->atime = ts;
		fsnodes_update_checksum(p);
		changelog(metaversion++,"%" PRIu32 "|ACCESS(%" PRIu32 ")",ts,inode);
/*
#ifdef CACHENOTIFY
		fsnodes_attr_changed(p,ts);
#endif
*/
	}
	stats_read++;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint8_t fs_writechunk(uint32_t inode, uint32_t indx, bool usedummylockid,
		uint64_t *chunkid, uint64_t *length, uint8_t *opflag, /* inout */ uint32_t *lockid) {
	int status;
	uint32_t i;
	uint64_t ochunkid,nchunkid;
	statsrecord psr,nsr;
	fsedge *e;
	fsnode *p;
	uint32_t ts = main_time();

	*chunkid = 0;
	*length = 0;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	if (indx>MAX_INDEX) {
		return ERROR_INDEXTOOBIG;
	}
	const bool quota_exceeded = fsnodes_size_quota_exceeded(p->uid, p->gid);
	fsnodes_get_stats(p,&psr);
	/* resize chunks structure */
	if (indx>=p->data.fdata.chunks) {
		if (quota_exceeded) {
			return ERROR_QUOTA;
		}
		uint32_t newsize;
		if (indx<8) {
			newsize=indx+1;
		} else if (indx<64) {
			newsize=(indx&0xFFFFFFF8)+8;
		} else {
			newsize = (indx&0xFFFFFFC0)+64;
		}
		if (p->data.fdata.chunktab==NULL) {
			p->data.fdata.chunktab = (uint64_t*)malloc(sizeof(uint64_t)*newsize);
		} else {
			p->data.fdata.chunktab = (uint64_t*)realloc(p->data.fdata.chunktab,sizeof(uint64_t)*newsize);
		}
		passert(p->data.fdata.chunktab);
		for (i=p->data.fdata.chunks ; i<newsize ; i++) {
			p->data.fdata.chunktab[i]=0;
		}
		p->data.fdata.chunks = newsize;
	}
	ochunkid = p->data.fdata.chunktab[indx];
	status = chunk_multi_modify(&nchunkid, ochunkid, p->goal, opflag, lockid, usedummylockid,
			quota_exceeded);
	if (status!=STATUS_OK) {
		fsnodes_update_checksum(p);
		return status;
	}
	p->data.fdata.chunktab[indx] = nchunkid;
	fsnodes_get_stats(p,&nsr);
	for (e=p->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	fsnodes_quota_update_size(p, nsr.size - psr.size);
	*chunkid = nchunkid;
	*length = p->data.fdata.length;
	changelog(metaversion++,"%" PRIu32 "|WRITE(%" PRIu32 ",%" PRIu32 ",%" PRIu8 ",%" PRIu32 "):%" PRIu64,ts,inode,indx,*opflag,lockid,nchunkid);
	if (p->mtime!=ts || p->ctime!=ts) {
		p->mtime = p->ctime = ts;
/*
#ifdef CACHENOTIFY
		fsnodes_attr_changed(p,ts);
#endif
*/
	}
	fsnodes_update_checksum(p);
	stats_write++;
	return STATUS_OK;
}
#else
uint8_t fs_write(uint32_t ts, uint32_t inode, uint32_t indx,
		uint8_t opflag, uint64_t chunkid, uint32_t lockid) {
	int status;
	uint32_t i;
	uint64_t ochunkid,nchunkid;
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	if (indx>MAX_INDEX) {
		return ERROR_INDEXTOOBIG;
	}
	/* resize chunks structure */
	if (indx>=p->data.fdata.chunks) {
		uint32_t newsize;
		if (indx<8) {
			newsize=indx+1;
		} else if (indx<64) {
			newsize=(indx&0xFFFFFFF8)+8;
		} else {
			newsize = (indx&0xFFFFFFC0)+64;
		}
		if (p->data.fdata.chunktab==NULL) {
			p->data.fdata.chunktab = (uint64_t*)malloc(sizeof(uint64_t)*newsize);
		} else {
			p->data.fdata.chunktab = (uint64_t*)realloc(p->data.fdata.chunktab,sizeof(uint64_t)*newsize);
		}
		passert(p->data.fdata.chunktab);
		for (i=p->data.fdata.chunks ; i<newsize ; i++) {
			p->data.fdata.chunktab[i]=0;
		}
		p->data.fdata.chunks = newsize;
	}
	ochunkid = p->data.fdata.chunktab[indx];
	status = chunk_multi_modify(ts,&nchunkid,ochunkid,p->goal,opflag,lockid);
	if (status!=STATUS_OK) {
		fsnodes_update_checksum(p);
		return status;
	}
	if (nchunkid!=chunkid) {
		fsnodes_update_checksum(p);
		return ERROR_MISMATCH;
	}
	p->data.fdata.chunktab[indx] = nchunkid;
	metaversion++;
	p->mtime = p->ctime = ts;
	fsnodes_update_checksum(p);
	return STATUS_OK;
}
#endif


#ifndef METARESTORE
uint8_t fs_writeend(uint32_t inode,uint64_t length,uint64_t chunkid, uint32_t lockid) {
	uint32_t ts = main_time();
	uint8_t status = chunk_can_unlock(chunkid, lockid);
	if (status != STATUS_OK) {
		return status;
	}
	if (length>0) {
		fsnode *p;
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
		if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
			return ERROR_EPERM;
		}
		if (length>p->data.fdata.length) {
			fsnodes_setlength(p,length);
			p->mtime = p->ctime = ts;
			fsnodes_update_checksum(p);
			changelog(metaversion++,"%" PRIu32 "|LENGTH(%" PRIu32 ",%" PRIu64 ")",ts,inode,length);
/*
#ifdef CACHENOTIFY
			fsnodes_attr_changed(p,ts);
#endif
*/
		}
	}
	changelog(metaversion++,"%" PRIu32 "|UNLOCK(%" PRIu64 ")",ts,chunkid);
	return chunk_unlock(chunkid);
}
#endif

#ifndef METARESTORE
void fs_incversion(uint64_t chunkid) {
	changelog(metaversion++,"%" PRIu32 "|INCVERSION(%" PRIu64 ")",(uint32_t)main_time(),chunkid);
}
#else
uint8_t fs_incversion(uint64_t chunkid) {
	metaversion++;
	return chunk_increase_version(chunkid);
}
#endif


#ifndef METARESTORE
uint8_t fs_repair(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t *notchanged,uint32_t *erased,uint32_t *repaired) {
	uint32_t nversion,indx;
	statsrecord psr,nsr;
	fsedge *e;
	fsnode *p,*rn;
	uint32_t ts = main_time();

	*notchanged = 0;
	*erased = 0;
	*repaired = 0;
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (rootinode==MFS_ROOT_ID || rootinode==0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
		if (rootinode==0 && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
			return ERROR_EPERM;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	if (!fsnodes_access(p,uid,gid,MODE_MASK_W,sesflags)) {
		return ERROR_EACCES;
	}
	fsnodes_get_stats(p,&psr);
	for (indx=0 ; indx<p->data.fdata.chunks ; indx++) {
		if (chunk_repair(p->goal,p->data.fdata.chunktab[indx],&nversion)) {
			changelog(metaversion++,"%" PRIu32 "|REPAIR(%" PRIu32 ",%" PRIu32 "):%" PRIu32,ts,inode,indx,nversion);
			p->mtime = p->ctime = ts;
			if (nversion>0) {
				(*repaired)++;
			} else {
				p->data.fdata.chunktab[indx] = 0;
				(*erased)++;
			}
		} else {
			(*notchanged)++;
		}
	}
	fsnodes_get_stats(p,&nsr);
	for (e=p->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	fsnodes_quota_update_size(p, nsr.size - psr.size);
	fsnodes_update_checksum(p);
	return STATUS_OK;
}
#else
uint8_t fs_repair(uint32_t ts,uint32_t inode,uint32_t indx,uint32_t nversion) {
	fsnode *p;
	uint8_t status;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	if (indx>MAX_INDEX) {
		return ERROR_INDEXTOOBIG;
	}
	if (indx>=p->data.fdata.chunks) {
		return ERROR_NOCHUNK;
	}
	if (p->data.fdata.chunktab[indx]==0) {
		return ERROR_NOCHUNK;
	}
	if (nversion==0) {
		status = chunk_delete_file(p->data.fdata.chunktab[indx],p->goal);
		p->data.fdata.chunktab[indx]=0;
	} else {
		status = chunk_set_version(p->data.fdata.chunktab[indx],nversion);
	}
	metaversion++;
	p->mtime = p->ctime = ts;
	fsnodes_update_checksum(p);
	return status;
}
#endif

#ifndef METARESTORE
uint8_t fs_getgoal(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint8_t gmode,
		GoalStats& goalStats) {
	fsnode *p,*rn;
	(void)sesflags;
	if (!GMODE_ISVALID(gmode)) {
		return ERROR_EINVAL;
	}
	if (rootinode==MFS_ROOT_ID || rootinode==0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
		if (rootinode==0 && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
			return ERROR_EPERM;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	fsnodes_getgoal_recursive(p, gmode, goalStats);
	return STATUS_OK;
}

uint8_t fs_gettrashtime_prepare(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t gmode,void **fptr,void **dptr,uint32_t *fnodes,uint32_t *dnodes) {
	fsnode *p,*rn;
	bstnode *froot,*droot;
	(void)sesflags;
	froot = NULL;
	droot = NULL;
	*fptr = NULL;
	*dptr = NULL;
	*fnodes = 0;
	*dnodes = 0;
	if (!GMODE_ISVALID(gmode)) {
		return ERROR_EINVAL;
	}
	if (rootinode==MFS_ROOT_ID || rootinode==0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
		if (rootinode==0 && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
			return ERROR_EPERM;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	fsnodes_gettrashtime_recursive(p,gmode,&froot,&droot);
	*fptr = froot;
	*dptr = droot;
	*fnodes = fsnodes_bst_nodes(froot);
	*dnodes = fsnodes_bst_nodes(droot);
	return STATUS_OK;
}

void fs_gettrashtime_store(void *fptr,void *dptr,uint8_t *buff) {
	bstnode *froot,*droot;
	froot = (bstnode*)fptr;
	droot = (bstnode*)dptr;
	fsnodes_bst_storedata(froot,&buff);
	fsnodes_bst_storedata(droot,&buff);
	fsnodes_bst_free(froot);
	fsnodes_bst_free(droot);
}

uint8_t fs_geteattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t gmode,uint32_t feattrtab[16],uint32_t deattrtab[16]) {
	fsnode *p,*rn;
	(void)sesflags;
	memset(feattrtab,0,16*sizeof(uint32_t));
	memset(deattrtab,0,16*sizeof(uint32_t));
	if (!GMODE_ISVALID(gmode)) {
		return ERROR_EINVAL;
	}
	if (rootinode==MFS_ROOT_ID || rootinode==0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
		if (rootinode==0 && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
			return ERROR_EPERM;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	fsnodes_geteattr_recursive(p,gmode,feattrtab,deattrtab);
	return STATUS_OK;
}

#endif

#ifndef METARESTORE
uint8_t fs_setgoal(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t goal,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	uint32_t ts;
	fsnode *rn;
#else
uint8_t fs_setgoal(uint32_t ts,uint32_t inode,uint32_t uid,uint8_t goal,uint8_t smode,uint32_t sinodes,uint32_t ncinodes,uint32_t nsinodes) {
	uint32_t si,nci,nsi;
#endif
	fsnode *p;

#ifndef METARESTORE
	(void)sesflags;
	ts = main_time();
	*sinodes = 0;
	*ncinodes = 0;
	*nsinodes = 0;
#else
	si = 0;
	nci = 0;
	nsi = 0;
#endif
	if (!SMODE_ISVALID(smode) || !isGoalValid(goal) ||
			(smode & (SMODE_INCREASE | SMODE_DECREASE) && isXorGoal(goal))) {
		return ERROR_EINVAL;
	}
#ifndef METARESTORE
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (rootinode==MFS_ROOT_ID || rootinode==0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
		if (rootinode==0 && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
			return ERROR_EPERM;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
#else
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
#endif
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}

#ifndef METARESTORE
	fsnodes_setgoal_recursive(p,ts,uid,goal,smode,sinodes,ncinodes,nsinodes);
	if ((smode&SMODE_RMASK)==0 && *nsinodes>0 && *sinodes==0 && *ncinodes==0) {
		return ERROR_EPERM;
	}
#else
	fsnodes_setgoal_recursive(p,ts,uid,goal,smode,&si,&nci,&nsi);
#endif

#ifndef METARESTORE
	changelog(metaversion++,"%" PRIu32 "|SETGOAL(%" PRIu32 ",%" PRIu32 ",%" PRIu8 ",%" PRIu8 "):%" PRIu32 ",%" PRIu32 ",%" PRIu32,ts,inode,uid,goal,smode,*sinodes,*ncinodes,*nsinodes);
	return STATUS_OK;
#else
	metaversion++;
	if (sinodes!=si || ncinodes!=nci || nsinodes!=nsi) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
#endif
}

#ifndef METARESTORE
uint8_t fs_settrashtime(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	uint32_t ts;
	fsnode *rn;
#else
uint8_t fs_settrashtime(uint32_t ts,uint32_t inode,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t sinodes,uint32_t ncinodes,uint32_t nsinodes) {
	uint32_t si,nci,nsi;
#endif
	fsnode *p;

#ifndef METARESTORE
	(void)sesflags;
	ts = main_time();
	*sinodes = 0;
	*ncinodes = 0;
	*nsinodes = 0;
#else
	si = 0;
	nci = 0;
	nsi = 0;
#endif
	if (!SMODE_ISVALID(smode)) {
		return ERROR_EINVAL;
	}
#ifndef METARESTORE
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (rootinode==MFS_ROOT_ID || rootinode==0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
		if (rootinode==0 && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
			return ERROR_EPERM;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
#else
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
#endif
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}

#ifndef METARESTORE
	fsnodes_settrashtime_recursive(p,ts,uid,trashtime,smode,sinodes,ncinodes,nsinodes);
	if ((smode&SMODE_RMASK)==0 && *nsinodes>0 && *sinodes==0 && *ncinodes==0) {
		return ERROR_EPERM;
	}
#else
	fsnodes_settrashtime_recursive(p,ts,uid,trashtime,smode,&si,&nci,&nsi);
#endif

#ifndef METARESTORE
	changelog(metaversion++,"%" PRIu32 "|SETTRASHTIME(%" PRIu32 ",%" PRIu32 ",%" PRIu32 ",%" PRIu8 "):%" PRIu32 ",%" PRIu32 ",%" PRIu32,ts,inode,uid,trashtime,smode,*sinodes,*ncinodes,*nsinodes);
	return STATUS_OK;
#else
	metaversion++;
	if (sinodes!=si || ncinodes!=nci || nsinodes!=nsi) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
#endif
}

#ifndef METARESTORE
uint8_t fs_seteattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t eattr,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	uint32_t ts;
	fsnode *rn;
#else
uint8_t fs_seteattr(uint32_t ts,uint32_t inode,uint32_t uid,uint8_t eattr,uint8_t smode,uint32_t sinodes,uint32_t ncinodes,uint32_t nsinodes) {
	uint32_t si,nci,nsi;
#endif
	fsnode *p;

#ifndef METARESTORE
	(void)sesflags;
	ts = main_time();
	*sinodes = 0;
	*ncinodes = 0;
	*nsinodes = 0;
#else
	si = 0;
	nci = 0;
	nsi = 0;
#endif
	if (!SMODE_ISVALID(smode) || (eattr&(~(EATTR_NOOWNER|EATTR_NOACACHE|EATTR_NOECACHE|EATTR_NODATACACHE)))) {
		return ERROR_EINVAL;
	}
#ifndef METARESTORE
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (rootinode==MFS_ROOT_ID || rootinode==0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
		if (rootinode==0 && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
			return ERROR_EPERM;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
#else
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
#endif

#ifndef METARESTORE
	fsnodes_seteattr_recursive(p,ts,uid,eattr,smode,sinodes,ncinodes,nsinodes);
	if ((smode&SMODE_RMASK)==0 && *nsinodes>0 && *sinodes==0 && *ncinodes==0) {
		return ERROR_EPERM;
	}
#else
	fsnodes_seteattr_recursive(p,ts,uid,eattr,smode,&si,&nci,&nsi);
#endif

#ifndef METARESTORE
	changelog(metaversion++,"%" PRIu32 "|SETEATTR(%" PRIu32 ",%" PRIu32 ",%" PRIu8 ",%" PRIu8 "):%" PRIu32 ",%" PRIu32 ",%" PRIu32/*",%" PRIu32*/,ts,inode,uid,eattr,smode,*sinodes,*ncinodes,*nsinodes/*,*qeinodes*/);
	return STATUS_OK;
#else
	metaversion++;
	if (sinodes!=si || ncinodes!=nci || nsinodes!=nsi/* || qeinodes!=qei*/) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
#endif
}

#ifndef METARESTORE

uint8_t fs_listxattr_leng(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,void **xanode,uint32_t *xasize) {
	fsnode *p,*rn;

	*xasize = 0;
	if (rootinode==MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	if (opened==0) {
		if (!fsnodes_access(p,uid,gid,MODE_MASK_R,sesflags)) {
			return ERROR_EACCES;
		}
	}
	return xattr_listattr_leng(inode,xanode,xasize);
}

void fs_listxattr_data(void *xanode,uint8_t *xabuff) {
	xattr_listattr_data(xanode,xabuff);
}

uint8_t fs_setxattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t anleng,const uint8_t *attrname,uint32_t avleng,const uint8_t *attrvalue,uint8_t mode) {
	uint32_t ts;
	fsnode *p,*rn;
	uint8_t status;

	ts = main_time();
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (rootinode==MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	if (opened==0) {
		if (!fsnodes_access(p,uid,gid,MODE_MASK_W,sesflags)) {
			return ERROR_EACCES;
		}
	}
	if (xattr_namecheck(anleng,attrname)<0) {
		return ERROR_EINVAL;
	}
	if (mode>MFS_XATTR_REMOVE) {
		return ERROR_EINVAL;
	}
	status = xattr_setattr(inode,anleng,attrname,avleng,attrvalue,mode);
	if (status!=STATUS_OK) {
		return status;
	}
	p->ctime = ts;
	fsnodes_update_checksum(p);
	changelog(metaversion++,"%" PRIu32 "|SETXATTR(%" PRIu32 ",%s,%s,%" PRIu8 ")",ts,inode,fsnodes_escape_name(anleng,attrname),fsnodes_escape_name(avleng,attrvalue),mode);
	return STATUS_OK;
}

uint8_t fs_getxattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t anleng,const uint8_t *attrname,uint32_t *avleng,uint8_t **attrvalue) {
	fsnode *p,*rn;

	if (rootinode==MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	if (opened==0) {
		if (!fsnodes_access(p,uid,gid,MODE_MASK_R,sesflags)) {
			return ERROR_EACCES;
		}
	}
	if (xattr_namecheck(anleng,attrname)<0) {
		return ERROR_EINVAL;
	}
	return xattr_getattr(inode,anleng,attrname,avleng,attrvalue);
}

#else /* METARESTORE */

uint8_t fs_setxattr(uint32_t ts,uint32_t inode,uint32_t anleng,const uint8_t *attrname,uint32_t avleng,const uint8_t *attrvalue,uint32_t mode) {
	fsnode *p;
	uint8_t status;
	if (anleng==0 || anleng>MFS_XATTR_NAME_MAX || avleng>MFS_XATTR_SIZE_MAX || mode>MFS_XATTR_REMOVE) {
		return ERROR_EINVAL;
	}
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	status = xattr_setattr(inode,anleng,attrname,avleng,attrvalue,mode);

	if (status!=STATUS_OK) {
		return status;
	}
	p->ctime = ts;
	metaversion++;
	fsnodes_update_checksum(p);
	return status;
}

#endif

#ifndef METARESTORE
uint8_t fs_deleteacl(uint32_t rootinode, uint8_t sesflags,
		uint32_t inode, uint32_t uid, uint32_t gid, AclType type) {
	uint32_t ts = main_time();
	fsnode *p;
	uint8_t status = fsnodes_get_node_for_operation(rootinode, sesflags, uid, gid,
			MODE_MASK_EMPTY, OperationType::kChangesMetadata, ExpectedInodeType::kOnlyLinked,
			&inode, &p);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_deleteacl(p, type, ts);
	if (status == STATUS_OK) {
		changelog(metaversion++,
				"%" PRIu32 "|DELETEACL(%" PRIu32 ",%c)",
				ts, inode, (type == AclType::kAccess ? 'a' : 'd'));
	}
	return status;

}

uint8_t fs_getacl(uint32_t rootinode, uint8_t sesflags,
		uint32_t inode, uint32_t uid, uint32_t gid, AclType type, AccessControlList& acl) {
	fsnode *p;
	uint8_t status = fsnodes_get_node_for_operation(rootinode, sesflags, uid, gid,
			MODE_MASK_EMPTY, OperationType::kReadOnly,
			ExpectedInodeType::kOnlyLinked, &inode, &p);
	if (status != STATUS_OK) {
		return status;
	}
	return fsnodes_getacl(p, type, acl);
}

uint8_t fs_setacl(uint32_t rootinode, uint8_t sesflags,
		uint32_t inode, uint32_t uid, uint32_t gid, AclType type, AccessControlList acl) {
	uint32_t ts = main_time();
	fsnode *p;
	uint8_t status = fsnodes_get_node_for_operation(rootinode, sesflags, uid, gid,
			MODE_MASK_EMPTY, OperationType::kChangesMetadata, ExpectedInodeType::kOnlyLinked,
			&inode, &p);
	if (status != STATUS_OK) {
		return status;
	}
	std::string aclString = acl.toString();
	status = fsnodes_setacl(p, type, std::move(acl), ts);
	if (status == STATUS_OK) {
		changelog(metaversion++,
				"%" PRIu32 "|SETACL(%" PRIu32 ",%c,%s)",
				ts, inode, (type == AclType::kAccess ? 'a' : 'd'), aclString.c_str());
	}
	return status;
}
#else
uint8_t fs_deleteacl(uint32_t ts, uint32_t inode, char aclType) {
	fsnode *p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}

	uint8_t status = ERROR_EINVAL;
	if (aclType == 'd') {
		status = fsnodes_deleteacl(p, AclType::kDefault, ts);
	} else if (aclType == 'a') {
		status = fsnodes_deleteacl(p, AclType::kAccess, ts);
	}
	if (status == STATUS_OK) {
		metaversion++;
	}
	return status;
}

uint8_t fs_setacl(uint32_t ts, uint32_t inode, char aclType, const char *aclString) {
	AccessControlList acl;
	try {
		acl = AccessControlList::fromString(aclString);
	} catch (Exception&) {
		return ERROR_EINVAL;
	}
	fsnode *p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	AclType aclTypeEnum;
	if (!decodeChar("da", {AclType::kDefault, AclType::kAccess}, aclType, aclTypeEnum)) {
		return ERROR_EINVAL;
	}
	uint8_t status = fsnodes_setacl(p, aclTypeEnum, std::move(acl), ts);
	if (status == STATUS_OK) {
		metaversion++;
	}
	return status;
}
#endif

#ifndef METARESTORE
uint8_t fs_quota_get_all(uint8_t sesflags, uint32_t uid,
		std::vector<QuotaOwnerAndLimits>& results) {
	if (uid != 0 && !(sesflags & SESFLAG_ALLCANCHANGEQUOTA)) {
		return ERROR_EPERM;
	}
	results = gQuotaDatabase.getAll();
	return STATUS_OK;
}

uint8_t fs_quota_get(uint8_t sesflags, uint32_t uid, uint32_t gid,
		const std::vector<QuotaOwner>& owners, std::vector<QuotaOwnerAndLimits>& results) {
	std::vector<QuotaOwnerAndLimits> tmp;
	for (const QuotaOwner& owner : owners) {
		if (uid != 0 && !(sesflags & SESFLAG_ALLCANCHANGEQUOTA)) {
			switch (owner.ownerType) {
			case QuotaOwnerType::kUser:
				if (uid != owner.ownerId) {
					return ERROR_EPERM;
				}
				break;
			case QuotaOwnerType::kGroup:
				if (gid != owner.ownerId && !(sesflags & SESFLAG_IGNOREGID)) {
					return ERROR_EPERM;
				}
				break;
			default:
				return ERROR_EINVAL;
			}
		}
		const QuotaLimits *result = gQuotaDatabase.get(owner.ownerType, owner.ownerId);
		if (result) {
			tmp.emplace_back(owner, *result);
		}
	}
	results.swap(tmp);
	return STATUS_OK;
}

uint8_t fs_quota_set(uint8_t sesflags, uint32_t uid, const std::vector<QuotaEntry>& entries) {
	if (uid != 0 && !(sesflags & SESFLAG_ALLCANCHANGEQUOTA)) {
		return ERROR_EPERM;
	}
	for (const QuotaEntry& entry : entries) {
		const QuotaOwner& owner = entry.entryKey.owner;
		gQuotaDatabase.set(entry.entryKey.rigor, entry.entryKey.resource, owner.ownerType,
				owner.ownerId, entry.limit);
		changelog(metaversion++, "%" PRIu32 "|SETQUOTA(%c,%c,%c,%" PRIu32 ",%" PRIu64 ")",
				main_time(),
				(entry.entryKey.rigor == QuotaRigor::kSoft)? 'S' : 'H',
				(entry.entryKey.resource == QuotaResource::kSize)? 'S' : 'I',
				(owner.ownerType == QuotaOwnerType::kUser)? 'U' : 'G',
				uint32_t{owner.ownerId}, uint64_t{entry.limit});
	}
	return STATUS_OK;
}
#else
uint8_t fs_quota_set(char rigor, char resource, char ownerType, uint32_t ownerId, uint64_t limit) {
	QuotaRigor quotaRigor;
	QuotaResource quotaResource;
	QuotaOwnerType quotaOwnerType;
	bool valid = true;
	valid &= decodeChar("SH", {QuotaRigor::kSoft, QuotaRigor::kHard}, rigor, quotaRigor);
	valid &= decodeChar("SI", {QuotaResource::kSize, QuotaResource::kInodes}, resource,
				quotaResource);
	valid &= decodeChar("UG", {QuotaOwnerType::kUser, QuotaOwnerType::kGroup}, ownerType,
				quotaOwnerType);
	if (!valid) {
		return ERROR_EINVAL;
	}
	metaversion++;
	gQuotaDatabase.set(quotaRigor, quotaResource, quotaOwnerType, ownerId, limit);
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint32_t fs_getdirpath_size(uint32_t inode) {
	fsnode *node;
	node = fsnodes_id_to_node(inode);
	if (node) {
		if (node->type!=TYPE_DIRECTORY) {
			return 15; // "(not directory)"
		} else {
			return 1+fsnodes_getpath_size(node->parents);
		}
	} else {
		return 11; // "(not found)"
	}
	return 0;       // unreachable
}

void fs_getdirpath_data(uint32_t inode,uint8_t *buff,uint32_t size) {
	fsnode *node;
	node = fsnodes_id_to_node(inode);
	if (node) {
		if (node->type!=TYPE_DIRECTORY) {
			if (size>=15) {
				memcpy(buff,"(not directory)",15);
				return;
			}
		} else {
			if (size>0) {
				buff[0]='/';
				fsnodes_getpath_data(node->parents,buff+1,size-1);
				return;
			}
		}
	} else {
		if (size>=11) {
			memcpy(buff,"(not found)",11);
			return;
		}
	}
}

uint8_t fs_get_dir_stats(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t *inodes,uint32_t *dirs,uint32_t *files,uint32_t *chunks,uint64_t *length,uint64_t *size,uint64_t *rsize) {
	fsnode *p,*rn;
	statsrecord sr;
	(void)sesflags;
	if (rootinode==MFS_ROOT_ID || rootinode==0) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
		if (rootinode==0 && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
			return ERROR_EPERM;
		}
	} else {
		rn = fsnodes_id_to_node(rootinode);
		if (!rn || rn->type!=TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode==MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	fsnodes_get_stats(p,&sr);
	*inodes = sr.inodes;
	*dirs = sr.dirs;
	*files = sr.files;
	*chunks = sr.chunks;
	*length = sr.length;
	*size = sr.size;
	*rsize = sr.realsize;
//      syslog(LOG_NOTICE,"using fast stats");
	return STATUS_OK;
}
#endif

void fs_add_files_to_chunks() {
	uint32_t i,j;
	uint64_t chunkid;
	fsnode *f;
	for (i=0 ; i<NODEHASHSIZE ; i++) {
		for (f=nodehash[i] ; f ; f=f->next) {
			if (f->type==TYPE_FILE || f->type==TYPE_TRASH || f->type==TYPE_RESERVED) {
				for (j=0 ; j<f->data.fdata.chunks ; j++) {
					chunkid = f->data.fdata.chunktab[j];
					if (chunkid>0) {
						chunk_add_file(chunkid,f->goal);
					}
				}
			}
		}
	}
}

#ifndef METARESTORE

void fs_test_getdata(uint32_t *loopstart,uint32_t *loopend,uint32_t *files,uint32_t *ugfiles,uint32_t *mfiles,uint32_t *chunks,uint32_t *ugchunks,uint32_t *mchunks,char **msgbuff,uint32_t *msgbuffleng) {
	*loopstart = fsinfo_loopstart;
	*loopend = fsinfo_loopend;
	*files = fsinfo_files;
	*ugfiles = fsinfo_ugfiles;
	*mfiles = fsinfo_mfiles;
	*chunks = fsinfo_chunks;
	*ugchunks = fsinfo_ugchunks;
	*mchunks = fsinfo_mchunks;
	*msgbuff = fsinfo_msgbuff;
	*msgbuffleng = fsinfo_msgbuffleng;
}

uint32_t fs_test_log_inconsistency(fsedge *e,const char *iname,char *buff,uint32_t size) {
	uint32_t leng;
	leng=0;
	if (e->parent) {
		syslog(LOG_ERR,"structure error - %s inconsistency (edge: %" PRIu32 ",%s -> %" PRIu32 ")",iname,e->parent->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
		if (leng<size) {
			leng += snprintf(buff+leng,size-leng,"structure error - %s inconsistency (edge: %" PRIu32 ",%s -> %" PRIu32 ")\n",iname,e->parent->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
		}
	} else {
		if (e->child->type==TYPE_TRASH) {
			syslog(LOG_ERR,"structure error - %s inconsistency (edge: TRASH,%s -> %" PRIu32 ")",iname,fsnodes_escape_name(e->nleng,e->name),e->child->id);
			if (leng<size) {
				leng += snprintf(buff+leng,size-leng,"structure error - %s inconsistency (edge: TRASH,%s -> %" PRIu32 ")\n",iname,fsnodes_escape_name(e->nleng,e->name),e->child->id);
			}
		} else if (e->child->type==TYPE_RESERVED) {
			syslog(LOG_ERR,"structure error - %s inconsistency (edge: RESERVED,%s -> %" PRIu32 ")",iname,fsnodes_escape_name(e->nleng,e->name),e->child->id);
			if (leng<size) {
				leng += snprintf(buff+leng,size-leng,"structure error - %s inconsistency (edge: RESERVED,%s -> %" PRIu32 ")\n",iname,fsnodes_escape_name(e->nleng,e->name),e->child->id);
			}
		} else {
			syslog(LOG_ERR,"structure error - %s inconsistency (edge: NULL,%s -> %" PRIu32 ")",iname,fsnodes_escape_name(e->nleng,e->name),e->child->id);
			if (leng<size) {
				leng += snprintf(buff+leng,size-leng,"structure error - %s inconsistency (edge: NULL,%s -> %" PRIu32 ")\n",iname,fsnodes_escape_name(e->nleng,e->name),e->child->id);
			}
		}
	}
	return leng;
}

void fs_test_files() {
	static uint32_t i=0;
	uint32_t j;
	uint32_t k;
	uint64_t chunkid;
	uint8_t vc,valid,ugflag;
	static uint32_t files=0;
	static uint32_t ugfiles=0;
	static uint32_t mfiles=0;
	static uint32_t chunks=0;
	static uint32_t ugchunks=0;
	static uint32_t mchunks=0;
	static uint32_t errors=0;
	static uint32_t notfoundchunks=0;
	static uint32_t unavailchunks=0;
	static uint32_t unavailfiles=0;
	static uint32_t unavailtrashfiles=0;
	static uint32_t unavailreservedfiles=0;
	static char *msgbuff=NULL,*tmp;
	static uint32_t leng=0;
	fsnode *f;
	fsedge *e;

	if ((uint32_t)(main_time())<=test_start_time) {
		return;
	}
	if (i>=NODEHASHSIZE) {
		syslog(LOG_NOTICE,"structure check loop");
		i=0;
		errors=0;
	}
	if (i==0) {
		if (errors==ERRORS_LOG_MAX) {
			syslog(LOG_ERR,"only first %u errors (unavailable chunks/files) were logged",ERRORS_LOG_MAX);
			if (leng<MSGBUFFSIZE) {
				leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"only first %u errors (unavailable chunks/files) were logged\n",ERRORS_LOG_MAX);
			}
		}
		if (notfoundchunks>0) {
			syslog(LOG_ERR,"unknown chunks: %" PRIu32,notfoundchunks);
			if (leng<MSGBUFFSIZE) {
				leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"unknown chunks: %" PRIu32 "\n",notfoundchunks);
			}
			notfoundchunks=0;
		}
		if (unavailchunks>0) {
			syslog(LOG_ERR,"unavailable chunks: %" PRIu32,unavailchunks);
			if (leng<MSGBUFFSIZE) {
				leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"unavailable chunks: %" PRIu32 "\n",unavailchunks);
			}
			unavailchunks=0;
		}
		if (unavailtrashfiles>0) {
			syslog(LOG_ERR,"unavailable trash files: %" PRIu32,unavailtrashfiles);
			if (leng<MSGBUFFSIZE) {
				leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"unavailable trash files: %" PRIu32 "\n",unavailtrashfiles);
			}
			unavailtrashfiles=0;
		}
		if (unavailreservedfiles>0) {
			syslog(LOG_ERR,"unavailable reserved files: %" PRIu32,unavailreservedfiles);
			if (leng<MSGBUFFSIZE) {
				leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"unavailable reserved files: %" PRIu32 "\n",unavailreservedfiles);
			}
			unavailreservedfiles=0;
		}
		if (unavailfiles>0) {
			syslog(LOG_ERR,"unavailable files: %" PRIu32,unavailfiles);
			if (leng<MSGBUFFSIZE) {
				leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"unavailable files: %" PRIu32 "\n",unavailfiles);
			}
			unavailfiles=0;
		}
		fsinfo_files=files;
		fsinfo_ugfiles=ugfiles;
		fsinfo_mfiles=mfiles;
		fsinfo_chunks=chunks;
		fsinfo_ugchunks=ugchunks;
		fsinfo_mchunks=mchunks;
		files=0;
		ugfiles=0;
		mfiles=0;
		chunks=0;
		ugchunks=0;
		mchunks=0;

		if (fsinfo_msgbuff==NULL) {
			fsinfo_msgbuff= (char*) malloc(MSGBUFFSIZE);
			passert(fsinfo_msgbuff);
		}
		tmp = fsinfo_msgbuff;
		fsinfo_msgbuff=msgbuff;
		msgbuff = tmp;
		if (leng>MSGBUFFSIZE) {
			fsinfo_msgbuffleng=MSGBUFFSIZE;
		} else {
			fsinfo_msgbuffleng=leng;
		}
		leng=0;

		fsinfo_loopstart = fsinfo_loopend;
		fsinfo_loopend = main_time();
	}
	for (k=0 ; k<(NODEHASHSIZE/14400) && i<NODEHASHSIZE ; k++,i++) {
		for (f=nodehash[i] ; f ; f=f->next) {
			if (f->type==TYPE_FILE || f->type==TYPE_TRASH || f->type==TYPE_RESERVED) {
				valid = 1;
				ugflag = 0;
				for (j=0 ; j<f->data.fdata.chunks ; j++) {
					chunkid = f->data.fdata.chunktab[j];
					if (chunkid>0) {
						if (chunk_get_validcopies(chunkid,&vc)!=STATUS_OK) {
							if (errors<ERRORS_LOG_MAX) {
								syslog(LOG_ERR,"structure error - chunk %016" PRIX64 " not found (inode: %" PRIu32 " ; index: %" PRIu32 ")",chunkid,f->id,j);
								if (leng<MSGBUFFSIZE) {
									leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - chunk %016" PRIX64 " not found (inode: %" PRIu32 " ; index: %" PRIu32 ")\n",chunkid,f->id,j);
								}
								errors++;
							}
							notfoundchunks++;
							if ((notfoundchunks%1000)==0) {
								syslog(LOG_ERR,"unknown chunks: %" PRIu32 " ...",notfoundchunks);
							}
							valid =0;
							mchunks++;
						} else if (vc==0) {
							if (errors<ERRORS_LOG_MAX) {
								syslog(LOG_ERR,"currently unavailable chunk %016" PRIX64 " (inode: %" PRIu32 " ; index: %" PRIu32 ")",chunkid,f->id,j);
								if (leng<MSGBUFFSIZE) {
									leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"currently unavailable chunk %016" PRIX64 " (inode: %" PRIu32 " ; index: %" PRIu32 ")\n",chunkid,f->id,j);
								}
								errors++;
							}
							unavailchunks++;
							if ((unavailchunks%1000)==0) {
								syslog(LOG_ERR,"unavailable chunks: %" PRIu32 " ...",unavailchunks);
							}
							valid = 0;
							mchunks++;
						} else if ((isXorGoal(f->goal) && vc == 1) || (isOrdinaryGoal(f->goal) && vc < f->goal)) {
							ugflag = 1;
							ugchunks++;
						}
						chunks++;
					}
				}
				if (valid==0) {
					mfiles++;
					if (f->type==TYPE_TRASH) {
						if (errors<ERRORS_LOG_MAX) {
							syslog(LOG_ERR,"- currently unavailable file in trash %" PRIu32 ": %s",f->id,fsnodes_escape_name(f->parents->nleng,f->parents->name));
							if (leng<MSGBUFFSIZE) {
								leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"- currently unavailable file in trash %" PRIu32 ": %s\n",f->id,fsnodes_escape_name(f->parents->nleng,f->parents->name));
							}
							errors++;
							unavailtrashfiles++;
							if ((unavailtrashfiles%1000)==0) {
								syslog(LOG_ERR,"unavailable trash files: %" PRIu32 " ...",unavailtrashfiles);
							}
						}
					} else if (f->type==TYPE_RESERVED) {
						if (errors<ERRORS_LOG_MAX) {
							syslog(LOG_ERR,"+ currently unavailable reserved file %" PRIu32 ": %s",f->id,fsnodes_escape_name(f->parents->nleng,f->parents->name));
							if (leng<MSGBUFFSIZE) {
								leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"+ currently unavailable reserved file %" PRIu32 ": %s\n",f->id,fsnodes_escape_name(f->parents->nleng,f->parents->name));
							}
							errors++;
							unavailreservedfiles++;
							if ((unavailreservedfiles%1000)==0) {
								syslog(LOG_ERR,"unavailable reserved files: %" PRIu32 " ...",unavailreservedfiles);
							}
						}
					} else {
						uint8_t *path;
						uint16_t pleng;
						for (e=f->parents ; e ; e=e->nextparent) {
							if (errors<ERRORS_LOG_MAX) {
								fsnodes_getpath(e,&pleng,&path);
								syslog(LOG_ERR,"* currently unavailable file %" PRIu32 ": %s",f->id,fsnodes_escape_name(pleng,path));
								if (leng<MSGBUFFSIZE) {
									leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"* currently unavailable file %" PRIu32 ": %s\n",f->id,fsnodes_escape_name(pleng,path));
								}
								free(path);
								errors++;
							}
							unavailfiles++;
							if ((unavailfiles%1000)==0) {
								syslog(LOG_ERR,"unavailable files: %" PRIu32 " ...",unavailfiles);
							}
						}
					}
				} else if (ugflag) {
					ugfiles++;
				}
				files++;
			}
			for (e=f->parents ; e ; e=e->nextparent) {
				if (e->child != f) {
					if (e->parent) {
						syslog(LOG_ERR,"structure error - edge->child/child->edges (node: %" PRIu32 " ; edge: %" PRIu32 ",%s -> %" PRIu32 ")",f->id,e->parent->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
						if (leng<MSGBUFFSIZE) {
							leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - edge->child/child->edges (node: %" PRIu32 " ; edge: %" PRIu32 ",%s -> %" PRIu32 ")\n",f->id,e->parent->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
						}
					} else {
						syslog(LOG_ERR,"structure error - edge->child/child->edges (node: %" PRIu32 " ; edge: NULL,%s -> %" PRIu32 ")",f->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
						if (leng<MSGBUFFSIZE) {
							leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - edge->child/child->edges (node: %" PRIu32 " ; edge: NULL,%s -> %" PRIu32 ")\n",f->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
						}
					}
				} else if (e->nextchild) {
					if (e->nextchild->prevchild != &(e->nextchild)) {
						if (leng<MSGBUFFSIZE) {
							leng += fs_test_log_inconsistency(e,"nextchild/prevchild",msgbuff+leng,MSGBUFFSIZE-leng);
						} else {
							fs_test_log_inconsistency(e,"nextchild/prevchild",NULL,0);
						}
					}
				} else if (e->nextparent) {
					if (e->nextparent->prevparent != &(e->nextparent)) {
						if (leng<MSGBUFFSIZE) {
							leng += fs_test_log_inconsistency(e,"nextparent/prevparent",msgbuff+leng,MSGBUFFSIZE-leng);
						} else {
							fs_test_log_inconsistency(e,"nextparent/prevparent",NULL,0);
						}
					}
#ifdef EDGEHASH
				} else if (e->next) {
					if (e->next->prev != &(e->next)) {
						if (leng<MSGBUFFSIZE) {
							leng += fs_test_log_inconsistency(e,"nexthash/prevhash",msgbuff+leng,MSGBUFFSIZE-leng);
						} else {
							fs_test_log_inconsistency(e,"nexthash/prevhash",NULL,0);
						}
					}
#endif
				}
			}
			if (f->type == TYPE_DIRECTORY) {
				for (e=f->data.ddata.children ; e ; e=e->nextchild) {
					if (e->parent != f) {
						if (e->parent) {
							syslog(LOG_ERR,"structure error - edge->parent/parent->edges (node: %" PRIu32 " ; edge: %" PRIu32 ",%s -> %" PRIu32 ")",f->id,e->parent->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
							if (leng<MSGBUFFSIZE) {
								leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - edge->parent/parent->edges (node: %" PRIu32 " ; edge: %" PRIu32 ",%s -> %" PRIu32 ")\n",f->id,e->parent->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
							}
						} else {
							syslog(LOG_ERR,"structure error - edge->parent/parent->edges (node: %" PRIu32 " ; edge: NULL,%s -> %" PRIu32 ")",f->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
							if (leng<MSGBUFFSIZE) {
								leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - edge->parent/parent->edges (node: %" PRIu32 " ; edge: NULL,%s -> %" PRIu32 ")\n",f->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
							}
						}
					} else if (e->nextchild) {
						if (e->nextchild->prevchild != &(e->nextchild)) {
							if (leng<MSGBUFFSIZE) {
								leng += fs_test_log_inconsistency(e,"nextchild/prevchild",msgbuff+leng,MSGBUFFSIZE-leng);
							} else {
								fs_test_log_inconsistency(e,"nextchild/prevchild",NULL,0);
							}
						}
					} else if (e->nextparent) {
						if (e->nextparent->prevparent != &(e->nextparent)) {
							if (leng<MSGBUFFSIZE) {
								leng += fs_test_log_inconsistency(e,"nextparent/prevparent",msgbuff+leng,MSGBUFFSIZE-leng);
							} else {
								fs_test_log_inconsistency(e,"nextparent/prevparent",NULL,0);
							}
						}
#ifdef EDGEHASH
					} else if (e->next) {
						if (e->next->prev != &(e->next)) {
							if (leng<MSGBUFFSIZE) {
								leng += fs_test_log_inconsistency(e,"nexthash/prevhash",msgbuff+leng,MSGBUFFSIZE-leng);
							} else {
								fs_test_log_inconsistency(e,"nexthash/prevhash",NULL,0);
							}
						}
#endif
					}
				}
			}
		}
	}
}
#endif


#ifndef METARESTORE
void fs_emptytrash(void) {
	uint32_t ts;
#else
uint8_t fs_emptytrash(uint32_t ts,uint32_t freeinodes,uint32_t reservedinodes) {
#endif
	uint32_t fi,ri;
	fsedge *e;
	fsnode *p;
#ifndef METARESTORE
	ts = main_time();
#endif
	fi=0;
	ri=0;
	e = trash;
	while (e) {
		p = e->child;
		e = e->nextchild;
		if (((uint64_t)(p->atime) + (uint64_t)(p->trashtime) < (uint64_t)ts) && ((uint64_t)(p->mtime) + (uint64_t)(p->trashtime) < (uint64_t)ts) && ((uint64_t)(p->ctime) + (uint64_t)(p->trashtime) < (uint64_t)ts)) {
			if (fsnodes_purge(ts,p)) {
				fi++;
			} else {
				ri++;
			}
		}
	}
#ifndef METARESTORE
	if ((fi|ri)>0) {
		changelog(metaversion++,"%" PRIu32 "|EMPTYTRASH():%" PRIu32 ",%" PRIu32,ts,fi,ri);
	}
#else
	metaversion++;
	if (freeinodes!=fi || reservedinodes!=ri) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
#endif
}

#ifndef METARESTORE
void fs_emptyreserved(void) {
	uint32_t ts;
#else
uint8_t fs_emptyreserved(uint32_t ts,uint32_t freeinodes) {
#endif
	fsedge *e;
	fsnode *p;
	uint32_t fi;
#ifndef METARESTORE
	ts = main_time();
#endif
	fi=0;
	e = reserved;
	while (e) {
		p = e->child;
		e = e->nextchild;
		if (p->data.fdata.sessionids==NULL) {
			fsnodes_purge(ts,p);
			fi++;
		}
	}
#ifndef METARESTORE
	if (fi>0) {
		changelog(metaversion++,"%" PRIu32 "|EMPTYRESERVED():%" PRIu32,ts,fi);
	}
#else
	metaversion++;
	if (freeinodes!=fi) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
#endif
}


#ifdef METARESTORE

uint64_t fs_getversion() {
	return metaversion;
}

#endif

enum {FLAG_TREE,FLAG_TRASH,FLAG_RESERVED};

#ifdef METARESTORE
/* DUMP */

void fs_dumpedge(fsedge *e) {
	if (e->parent==NULL) {
		if (e->child->type==TYPE_TRASH) {
			printf("E|p:     TRASH|c:%10" PRIu32 "|n:%s\n",e->child->id,fsnodes_escape_name(e->nleng,e->name));
		} else if (e->child->type==TYPE_RESERVED) {
			printf("E|p:  RESERVED|c:%10" PRIu32 "|n:%s\n",e->child->id,fsnodes_escape_name(e->nleng,e->name));
		} else {
			printf("E|p:      NULL|c:%10" PRIu32 "|n:%s\n",e->child->id,fsnodes_escape_name(e->nleng,e->name));
		}
	} else {
		printf("E|p:%10" PRIu32 "|c:%10" PRIu32 "|n:%s\n",e->parent->id,e->child->id,fsnodes_escape_name(e->nleng,e->name));
	}
}

void fs_dumpnode(fsnode *f) {
	char c;
	uint32_t i,ch;
	sessionidrec *sessionidptr;

	c='?';
	switch (f->type) {
	case TYPE_DIRECTORY:
		c='D';
		break;
	case TYPE_SOCKET:
		c='S';
		break;
	case TYPE_FIFO:
		c='F';
		break;
	case TYPE_BLOCKDEV:
		c='B';
		break;
	case TYPE_CHARDEV:
		c='C';
		break;
	case TYPE_SYMLINK:
		c='L';
		break;
	case TYPE_FILE:
		c='-';
		break;
	case TYPE_TRASH:
		c='T';
		break;
	case TYPE_RESERVED:
		c='R';
		break;
	}

	printf("%c|i:%10" PRIu32 "|#:%" PRIu8 "|e:%1" PRIX16 "|m:%04" PRIo16 "|u:%10" PRIu32 "|g:%10" PRIu32 "|a:%10" PRIu32 ",m:%10" PRIu32 ",c:%10" PRIu32 "|t:%10" PRIu32,c,f->id,f->goal,(uint16_t)(f->mode>>12),(uint16_t)(f->mode&0xFFF),f->uid,f->gid,f->atime,f->mtime,f->ctime,f->trashtime);

	if (f->type==TYPE_BLOCKDEV || f->type==TYPE_CHARDEV) {
		printf("|d:%5" PRIu32 ",%5" PRIu32 "\n",f->data.devdata.rdev>>16,f->data.devdata.rdev&0xFFFF);
	} else if (f->type==TYPE_SYMLINK) {
		printf("|p:%s\n",fsnodes_escape_name(f->data.sdata.pleng,f->data.sdata.path));
	} else if (f->type==TYPE_FILE || f->type==TYPE_TRASH || f->type==TYPE_RESERVED) {
		printf("|l:%20" PRIu64 "|c:(",f->data.fdata.length);
		ch = 0;
		for (i=0 ; i<f->data.fdata.chunks ; i++) {
			if (f->data.fdata.chunktab[i]!=0) {
				ch=i+1;
			}
		}
		for (i=0 ; i<ch ; i++) {
			if (f->data.fdata.chunktab[i]!=0) {
				printf("%016" PRIX64,f->data.fdata.chunktab[i]);
			} else {
				printf("N");
			}
			if (i+1<ch) {
				printf(",");
			}
		}
		printf(")|r:(");
		for (sessionidptr=f->data.fdata.sessionids ; sessionidptr ; sessionidptr=sessionidptr->next) {
			printf("%" PRIu32,sessionidptr->sessionid);
			if (sessionidptr->next) {
				printf(",");
			}
		}
		printf(")\n");
	} else {
		printf("\n");
	}
}

void fs_dumpnodes() {
	uint32_t i;
	fsnode *p;
	for (i=0 ; i<NODEHASHSIZE ; i++) {
		for (p=nodehash[i] ; p ; p=p->next) {
			fs_dumpnode(p);
		}
	}
}

void fs_dumpedgelist(fsedge *e) {
	while (e) {
		fs_dumpedge(e);
		e=e->nextchild;
	}
}

void fs_dumpedges(fsnode *f) {
	fsedge *e;
	fs_dumpedgelist(f->data.ddata.children);
	for (e=f->data.ddata.children ; e ; e=e->nextchild) {
		if (e->child->type==TYPE_DIRECTORY) {
			fs_dumpedges(e->child);
		}
	}
}

void fs_dumpfree() {
	freenode *n;
	for (n=freelist ; n ; n=n->next) {
		printf("I|i:%10" PRIu32 "|f:%10" PRIu32 "\n",n->id,n->ftime);
	}
}

void xattr_dump() {
	uint32_t i;
	xattr_data_entry *xa;

	for (i=0 ; i<XATTR_DATA_HASH_SIZE ; i++) {
		for (xa=xattr_data_hash[i] ; xa ; xa=xa->next) {
			printf("X|i:%10" PRIu32 "|n:%s|v:%s\n",xa->inode,fsnodes_escape_name(xa->anleng,xa->attrname),fsnodes_escape_name(xa->avleng,xa->attrvalue));
		}
	}
}


void fs_dump(void) {
	fs_dumpnodes();
	fs_dumpedges(root);
	fs_dumpedgelist(trash);
	fs_dumpedgelist(reserved);
	fs_dumpfree();
	xattr_dump();
}

#endif

void xattr_store(FILE *fd) {
	uint8_t hdrbuff[4+1+4];
	uint8_t *ptr;
	uint32_t i;
	xattr_data_entry *xa;

	for (i=0 ; i<XATTR_DATA_HASH_SIZE ; i++) {
		for (xa=xattr_data_hash[i] ; xa ; xa=xa->next) {
			ptr = hdrbuff;
			put32bit(&ptr,xa->inode);
			put8bit(&ptr,xa->anleng);
			put32bit(&ptr,xa->avleng);
			if (fwrite(hdrbuff,1,4+1+4,fd)!=(size_t)(4+1+4)) {
				syslog(LOG_NOTICE,"fwrite error");
				return;
			}
			if (fwrite(xa->attrname,1,xa->anleng,fd)!=(size_t)(xa->anleng)) {
				syslog(LOG_NOTICE,"fwrite error");
				return;
			}
			if (xa->avleng>0) {
				if (fwrite(xa->attrvalue,1,xa->avleng,fd)!=(size_t)(xa->avleng)) {
					syslog(LOG_NOTICE,"fwrite error");
					return;
				}
			}
		}
	}
	memset(hdrbuff,0,4+1+4);
	if (fwrite(hdrbuff,1,4+1+4,fd)!=(size_t)(4+1+4)) {
		syslog(LOG_NOTICE,"fwrite error");
		return;
	}
}

int xattr_load(FILE *fd,int ignoreflag) {
	uint8_t hdrbuff[4+1+4];
	const uint8_t *ptr;
	uint32_t inode;
	uint8_t anleng;
	uint32_t avleng;
	uint8_t nl=1;
	xattr_data_entry *xa;
	xattr_inode_entry *ih;
	uint32_t hash,ihash;

	while (1) {
		if (fread(hdrbuff,1,4+1+4,fd)!=4+1+4) {
			int err = errno;
			if (nl) {
				fputc('\n',stderr);
			}
			errno = err;
			mfs_errlog(LOG_ERR,"loading xattr: read error");
			return -1;
		}
		ptr = hdrbuff;
		inode = get32bit(&ptr);
		anleng = get8bit(&ptr);
		avleng = get32bit(&ptr);
		if (inode==0) {
			return 1;
		}
		if (anleng==0) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			mfs_syslog(LOG_ERR,"loading xattr: empty name");
			if (ignoreflag) {
				fseek(fd,anleng+avleng,SEEK_CUR);
				continue;
			} else {
				return -1;
			}
		}
		if (avleng>MFS_XATTR_SIZE_MAX) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			mfs_syslog(LOG_ERR,"loading xattr: value oversized");
			if (ignoreflag) {
				fseek(fd,anleng+avleng,SEEK_CUR);
				continue;
			} else {
				return -1;
			}
		}

		ihash = xattr_inode_hash_fn(inode);
		for (ih = xattr_inode_hash[ihash]; ih && ih->inode!=inode; ih=ih->next) {}

		if (ih && ih->anleng+anleng+1>MFS_XATTR_LIST_MAX) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			mfs_syslog(LOG_ERR,"loading xattr: name list too long");
			if (ignoreflag) {
				fseek(fd,anleng+avleng,SEEK_CUR);
				continue;
			} else {
				return -1;
			}
		}

		xa = (xattr_data_entry*) malloc(sizeof(xattr_data_entry));
		passert(xa);
		xa->inode = inode;
		xa->attrname = (uint8_t*) malloc(anleng);
		passert(xa->attrname);
		if (fread(xa->attrname,1,anleng,fd)!=(size_t)anleng) {
			int err = errno;
			if (nl) {
				fputc('\n',stderr);
			}
			free(xa->attrname);
			free(xa);
			errno = err;
			mfs_errlog(LOG_ERR,"loading xattr: read error");
			return -1;
		}
		xa->anleng = anleng;
		if (avleng>0) {
			xa->attrvalue = (uint8_t*) malloc(avleng);
			passert(xa->attrvalue);
			if (fread(xa->attrvalue,1,avleng,fd)!=(size_t)avleng) {
				int err = errno;
				if (nl) {
					fputc('\n',stderr);
				}
				free(xa->attrname);
				free(xa->attrvalue);
				free(xa);
				errno = err;
				mfs_errlog(LOG_ERR,"loading xattr: read error");
				return -1;
			}
		} else {
			xa->attrvalue = NULL;
		}
		xa->avleng = avleng;
		hash = xattr_data_hash_fn(inode,xa->anleng,xa->attrname);
		xa->next = xattr_data_hash[hash];
		if (xa->next) {
			xa->next->prev = &(xa->next);
		}
		xa->prev = xattr_data_hash + hash;
		xattr_data_hash[hash] = xa;

		if (ih) {
			xa->nextinode = ih->data_head;
			if (xa->nextinode) {
				xa->nextinode->previnode = &(xa->nextinode);
			}
			xa->previnode = &(ih->data_head);
			ih->data_head = xa;
			ih->anleng += anleng+1U;
			ih->avleng += avleng;
		} else {
			ih = (xattr_inode_entry*) malloc(sizeof(xattr_inode_entry));
			passert(ih);
			ih->inode = inode;
			xa->nextinode = NULL;
			xa->previnode = &(ih->data_head);
			ih->data_head = xa;
			ih->anleng = anleng+1U;
			ih->avleng = avleng;
			ih->next = xattr_inode_hash[ihash];
			xattr_inode_hash[ihash] = ih;
		}
	}
}

void xattr_init(void) {
	uint32_t i;
	xattr_data_hash = (xattr_data_entry**) malloc(sizeof(xattr_data_entry*)*XATTR_DATA_HASH_SIZE);
	passert(xattr_data_hash);
	for (i=0 ; i<XATTR_DATA_HASH_SIZE ; i++) {
		xattr_data_hash[i]=NULL;
	}
	xattr_inode_hash = (xattr_inode_entry**) malloc(sizeof(xattr_inode_entry*)*XATTR_INODE_HASH_SIZE);
	passert(xattr_inode_hash);
	for (i=0 ; i<XATTR_INODE_HASH_SIZE ; i++) {
		xattr_inode_hash[i]=NULL;
	}
}

template <class... Args>
static void fs_store_generic(FILE *fd, Args&&... args) {
	static std::vector<uint8_t> buffer;
	buffer.clear();
	const uint32_t size = serializedSize(std::forward<Args>(args)...);
	serialize(buffer, size, std::forward<Args>(args)...);
	if (fwrite(buffer.data(), 1, buffer.size(), fd) != buffer.size()) {
		syslog(LOG_NOTICE, "fwrite error");
		return;
	}
}

/* For future usage
static void fs_store_marker(FILE *fd) {
	const uint32_t zero = 0;
	if (fwrite(&zero, 1, 4, fd) != 4) {
		syslog(LOG_NOTICE, "fwrite error");
		return;
	}
}
*/

template <class... Args>
static bool fs_load_generic(FILE *fd, Args&&... args) {
	static std::vector<uint8_t> buffer;
	uint32_t size;
	buffer.resize(4);
	if (fread(buffer.data(), 1, 4, fd) != 4) {
		throw Exception("fread error (size)");
	}
	deserialize(buffer, size);
	if (size == 0) {
		// marker
		return false;
	}
	buffer.resize(size);
	if (fread(buffer.data(), 1, size, fd) != size) {
		throw Exception("fread error (entry)");
	}
	deserialize(buffer, std::forward<Args>(args)...);
	return true;
}


static void fs_storeacl(fsnode* p, FILE *fd) {
	static std::vector<uint8_t> buffer;
	buffer.clear();
	if (!p) {
		// write end marker
		uint32_t marker = 0;
		serialize(buffer, marker);
	} else {
		uint32_t size = serializedSize(p->id, p->extendedAcl, p->defaultAcl);
		serialize(buffer, size, p->id, p->extendedAcl, p->defaultAcl);
	}
	if (fwrite(buffer.data(), 1, buffer.size(), fd) != buffer.size()) {
		syslog(LOG_NOTICE, "fwrite error");
		return;
	}
}

static int fs_loadacl(FILE *fd, int ignoreflag) {
	static bool putLfBeforeError;
	static std::vector<uint8_t> buffer;

	// initialize
	if (fd == nullptr) {
		putLfBeforeError = true;
		return 0;
	}

	try {
		// Read size of the entry
		uint32_t size;
		buffer.resize(serializedSize(size));
		if (fread(buffer.data(), 1, buffer.size(), fd) != buffer.size()) {
			throw Exception(std::string("read error: ") + strerr(errno), ERROR_IO);
		}
		deserialize(buffer, size);
		if (size == 0) {
			// this is end marker
			return 1;
		} else if (size > 10000000) {
			throw Exception("strange size of entry: " + std::to_string(size), ERROR_ERANGE);
		}

		// Read the entry
		buffer.resize(size);
		if (fread(buffer.data(), 1, buffer.size(), fd) != buffer.size()) {
			throw Exception(std::string("read error: ") + strerr(errno), ERROR_IO);
		}

		// Deserialize inode
		uint32_t inode;
		deserialize(buffer, inode);
		fsnode* p = fsnodes_id_to_node(inode);
		if (!p) {
			throw Exception("unknown inode: " + std::to_string(inode));
		}

		// Deserialize ACL
		deserialize(buffer, inode, p->extendedAcl, p->defaultAcl);
		return 0;
	} catch (Exception& ex) {
		if (putLfBeforeError) {
			fputc('\n', stderr);
			putLfBeforeError = false;
		}
		mfs_arg_syslog(LOG_ERR, "loading acl: %s", ex.what());
		if (!ignoreflag || ex.status() != STATUS_OK) {
			return -1;
		} else {
			return 0;
		}
	}
}

void fs_storeedge(fsedge *e,FILE *fd) {
	uint8_t uedgebuff[4+4+2+65535];
	uint8_t *ptr;
	if (e==NULL) {  // last edge
		memset(uedgebuff,0,4+4+2);
		if (fwrite(uedgebuff,1,4+4+2,fd)!=(size_t)(4+4+2)) {
			syslog(LOG_NOTICE,"fwrite error");
			return;
		}
		return;
	}
	ptr = uedgebuff;
	if (e->parent==NULL) {
		put32bit(&ptr,0);
	} else {
		put32bit(&ptr,e->parent->id);
	}
	put32bit(&ptr,e->child->id);
	put16bit(&ptr,e->nleng);
	memcpy(ptr,e->name,e->nleng);
	if (fwrite(uedgebuff,1,4+4+2+e->nleng,fd)!=(size_t)(4+4+2+e->nleng)) {
		syslog(LOG_NOTICE,"fwrite error");
		return;
	}
}

int fs_loadedge(FILE *fd,int ignoreflag) {
	uint8_t uedgebuff[4+4+2];
	const uint8_t *ptr;
	uint32_t parent_id;
	uint32_t child_id;
#ifdef EDGEHASH
	uint32_t hpos;
#endif
	fsedge *e;
#ifndef METARESTORE
	statsrecord sr;
#endif
	static fsedge **root_tail;
	static fsedge **current_tail;
	static uint32_t current_parent_id;
	static uint8_t nl;

	if (fd==NULL) {
		current_parent_id = 0;
		current_tail = NULL;
		root_tail = NULL;
		nl = 1;
		return 0;
	}

	if (fread(uedgebuff,1,4+4+2,fd)!=4+4+2) {
		int err = errno;
		if (nl) {
			fputc('\n',stderr);
			nl=0;
		}
		errno = err;
		mfs_errlog(LOG_ERR,"loading edge: read error");
		return -1;
	}
	ptr = uedgebuff;
	parent_id = get32bit(&ptr);
	child_id = get32bit(&ptr);
	if (parent_id==0 && child_id==0) {      // last edge
		return 1;
	}
	e = (fsedge*) malloc(sizeof(fsedge));
	passert(e);
	e->nleng = get16bit(&ptr);
	if (e->nleng==0) {
		if (nl) {
			fputc('\n',stderr);
			nl=0;
		}
		mfs_arg_syslog(LOG_ERR,"loading edge: %" PRIu32 "->%" PRIu32 " error: empty name",parent_id,child_id);
		free(e);
		return -1;
	}
	e->name = (uint8_t*) malloc(e->nleng);
	passert(e->name);
	if (fread(e->name,1,e->nleng,fd)!=e->nleng) {
		int err = errno;
		if (nl) {
			fputc('\n',stderr);
			nl=0;
		}
		errno = err;
		mfs_errlog(LOG_ERR,"loading edge: read error");
		free(e->name);
		free(e);
		return -1;
	}
	e->child = fsnodes_id_to_node(child_id);
	if (e->child==NULL) {
		if (nl) {
			fputc('\n',stderr);
			nl=0;
		}
		mfs_arg_syslog(LOG_ERR,"loading edge: %" PRIu32 ",%s->%" PRIu32 " error: child not found",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
		free(e->name);
		free(e);
		if (ignoreflag) {
			return 0;
		}
		return -1;
	}
	if (parent_id==0) {
		if (e->child->type==TYPE_TRASH) {
			e->parent = NULL;
			e->nextchild = trash;
			if (e->nextchild) {
				e->nextchild->prevchild = &(e->nextchild);
			}
			trash = e;
			e->prevchild = &trash;
#ifdef EDGEHASH
			e->next = NULL;
			e->prev = NULL;
#endif
			trashspace += e->child->data.fdata.length;
			trashnodes++;
		} else if (e->child->type==TYPE_RESERVED) {
			e->parent = NULL;
			e->nextchild = reserved;
			if (e->nextchild) {
				e->nextchild->prevchild = &(e->nextchild);
			}
			reserved = e;
			e->prevchild = &reserved;
#ifdef EDGEHASH
			e->next = NULL;
			e->prev = NULL;
#endif
			reservedspace += e->child->data.fdata.length;
			reservednodes++;
		} else {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			fprintf(stderr,"loading edge: %" PRIu32 ",%s->%" PRIu32 " error: bad child type (%c)\n",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id,e->child->type);
#ifndef METARESTORE
			syslog(LOG_ERR,"loading edge: %" PRIu32 ",%s->%" PRIu32 " error: bad child type (%c)",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id,e->child->type);
#endif
			free(e->name);
			free(e);
			return -1;
		}
	} else {
		e->parent = fsnodes_id_to_node(parent_id);
		if (e->parent==NULL) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			fprintf(stderr,"loading edge: %" PRIu32 ",%s->%" PRIu32 " error: parent not found\n",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
#ifndef METARESTORE
			syslog(LOG_ERR,"loading edge: %" PRIu32 ",%s->%" PRIu32 " error: parent not found",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
#endif
			if (ignoreflag) {
				e->parent = fsnodes_id_to_node(MFS_ROOT_ID);
				if (e->parent==NULL || e->parent->type!=TYPE_DIRECTORY) {
					fprintf(stderr,"loading edge: %" PRIu32 ",%s->%" PRIu32 " root dir not found !!!\n",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
#ifndef METARESTORE
					syslog(LOG_ERR,"loading edge: %" PRIu32 ",%s->%" PRIu32 " root dir not found !!!",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
#endif
					free(e->name);
					free(e);
					return -1;
				}
				fprintf(stderr,"loading edge: %" PRIu32 ",%s->%" PRIu32 " attaching node to root dir\n",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
#ifndef METARESTORE
				syslog(LOG_ERR,"loading edge: %" PRIu32 ",%s->%" PRIu32 " attaching node to root dir",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
#endif
				parent_id = MFS_ROOT_ID;
			} else {
				fprintf(stderr,"use mfsmetarestore (option -i) to attach this node to root dir\n");
				free(e->name);
				free(e);
				return -1;
			}
		}
		if (e->parent->type!=TYPE_DIRECTORY) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			fprintf(stderr,"loading edge: %" PRIu32 ",%s->%" PRIu32 " error: bad parent type (%c)\n",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id,e->parent->type);
#ifndef METARESTORE
			syslog(LOG_ERR,"loading edge: %" PRIu32 ",%s->%" PRIu32 " error: bad parent type (%c)",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id,e->parent->type);
#endif
			if (ignoreflag) {
				e->parent = fsnodes_id_to_node(MFS_ROOT_ID);
				if (e->parent==NULL || e->parent->type!=TYPE_DIRECTORY) {
					fprintf(stderr,"loading edge: %" PRIu32 ",%s->%" PRIu32 " root dir not found !!!\n",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
#ifndef METARESTORE
					syslog(LOG_ERR,"loading edge: %" PRIu32 ",%s->%" PRIu32 " root dir not found !!!",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
#endif
					free(e->name);
					free(e);
					return -1;
				}
				fprintf(stderr,"loading edge: %" PRIu32 ",%s->%" PRIu32 " attaching node to root dir\n",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
#ifndef METARESTORE
				syslog(LOG_ERR,"loading edge: %" PRIu32 ",%s->%" PRIu32 " attaching node to root dir",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
#endif
				parent_id = MFS_ROOT_ID;
			} else {
				fprintf(stderr,"use mfsmetarestore (option -i) to attach this node to root dir\n");
				free(e->name);
				free(e);
				return -1;
			}
		}
		if (parent_id==MFS_ROOT_ID) {   // special case - because of 'ignoreflag' and possibility of attaching orphans into root node
			if (root_tail==NULL) {
				root_tail = &(e->parent->data.ddata.children);
			}
		} else if (current_parent_id!=parent_id) {
			if (e->parent->data.ddata.children) {
				if (nl) {
					fputc('\n',stderr);
					nl=0;
				}
				fprintf(stderr,"loading edge: %" PRIu32 ",%s->%" PRIu32 " error: parent node sequence error\n",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
#ifndef METARESTORE
				syslog(LOG_ERR,"loading edge: %" PRIu32 ",%s->%" PRIu32 " error: parent node sequence error",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
#endif
				if (ignoreflag) {
					current_tail = &(e->parent->data.ddata.children);
					while (*current_tail) {
						current_tail = &((*current_tail)->nextchild);
					}
				} else {
					free(e->name);
					free(e);
					return -1;
				}
			} else {
				current_tail = &(e->parent->data.ddata.children);
			}
			current_parent_id = parent_id;
		}
		e->nextchild = NULL;
		if (parent_id==MFS_ROOT_ID) {
			*(root_tail) = e;
			e->prevchild = root_tail;
			root_tail = &(e->nextchild);
		} else {
			*(current_tail) = e;
			e->prevchild = current_tail;
			current_tail = &(e->nextchild);
		}
		e->parent->data.ddata.elements++;
		if (e->child->type==TYPE_DIRECTORY) {
			e->parent->data.ddata.nlink++;
		}
#ifdef EDGEHASH
		hpos = EDGEHASHPOS(fsnodes_hash(e->parent->id,e->nleng,e->name));
		e->next = edgehash[hpos];
		if (e->next) {
			e->next->prev = &(e->next);
		}
		edgehash[hpos] = e;
		e->prev = &(edgehash[hpos]);
#endif
	}
	e->nextparent = e->child->parents;
	if (e->nextparent) {
		e->nextparent->prevparent = &(e->nextparent);
	}
	e->child->parents = e;
	e->prevparent = &(e->child->parents);
#ifndef METARESTORE
	if (e->parent) {
		fsnodes_get_stats(e->child,&sr);
		fsnodes_add_stats(e->parent,&sr);
	}
#endif
	return 0;
}

void fs_storenode(fsnode *f,FILE *fd) {
	uint8_t unodebuff[1+4+1+2+4+4+4+4+4+4+8+4+2+8*65536+4*65536+4];
	uint8_t *ptr,*chptr;
	uint32_t i,indx,ch,sessionids;
	sessionidrec *sessionidptr;

	if (f==NULL) {  // last node
		fputc(0,fd);
		return;
	}
	ptr = unodebuff;
	put8bit(&ptr,f->type);
	put32bit(&ptr,f->id);
	put8bit(&ptr,f->goal);
	put16bit(&ptr,f->mode);
	put32bit(&ptr,f->uid);
	put32bit(&ptr,f->gid);
	put32bit(&ptr,f->atime);
	put32bit(&ptr,f->mtime);
	put32bit(&ptr,f->ctime);
	put32bit(&ptr,f->trashtime);
	switch (f->type) {
	case TYPE_DIRECTORY:
	case TYPE_SOCKET:
	case TYPE_FIFO:
		if (fwrite(unodebuff,1,1+4+1+2+4+4+4+4+4+4,fd)!=(size_t)(1+4+1+2+4+4+4+4+4+4)) {
			syslog(LOG_NOTICE,"fwrite error");
			return;
		}
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		put32bit(&ptr,f->data.devdata.rdev);
		if (fwrite(unodebuff,1,1+4+1+2+4+4+4+4+4+4+4,fd)!=(size_t)(1+4+1+2+4+4+4+4+4+4+4)) {
			syslog(LOG_NOTICE,"fwrite error");
			return;
		}
		break;
	case TYPE_SYMLINK:
		put32bit(&ptr,f->data.sdata.pleng);
		if (fwrite(unodebuff,1,1+4+1+2+4+4+4+4+4+4+4,fd)!=(size_t)(1+4+1+2+4+4+4+4+4+4+4)) {
			syslog(LOG_NOTICE,"fwrite error");
			return;
		}
		if (fwrite(f->data.sdata.path,1,f->data.sdata.pleng,fd)!=(size_t)(f->data.sdata.pleng)) {
			syslog(LOG_NOTICE,"fwrite error");
			return;
		}
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		put64bit(&ptr,f->data.fdata.length);
		ch = 0;
		for (indx=0 ; indx<f->data.fdata.chunks ; indx++) {
			if (f->data.fdata.chunktab[indx]!=0) {
				ch=indx+1;
			}
		}
		put32bit(&ptr,ch);
		sessionids=0;
		for (sessionidptr=f->data.fdata.sessionids ; sessionidptr && sessionids<65535; sessionidptr=sessionidptr->next) {
			sessionids++;
		}
		put16bit(&ptr,sessionids);

		if (fwrite(unodebuff,1,1+4+1+2+4+4+4+4+4+4+8+4+2,fd)!=(size_t)(1+4+1+2+4+4+4+4+4+4+8+4+2)) {
			syslog(LOG_NOTICE,"fwrite error");
			return;
		}

		indx = 0;
		while (ch>65536) {
			chptr = ptr;
			for (i=0 ; i<65536 ; i++) {
				put64bit(&chptr,f->data.fdata.chunktab[indx]);
				indx++;
			}
			if (fwrite(ptr,1,8*65536,fd)!=(size_t)(8*65536)) {
				syslog(LOG_NOTICE,"fwrite error");
				return;
			}
			ch-=65536;
		}

		chptr = ptr;
		for (i=0 ; i<ch ; i++) {
			put64bit(&chptr,f->data.fdata.chunktab[indx]);
			indx++;
		}

		sessionids=0;
		for (sessionidptr=f->data.fdata.sessionids ; sessionidptr && sessionids<65535; sessionidptr=sessionidptr->next) {
			put32bit(&chptr,sessionidptr->sessionid);
			sessionids++;
		}

		if (fwrite(ptr,1,8*ch+4*sessionids,fd)!=(size_t)(8*ch+4*sessionids)) {
			syslog(LOG_NOTICE,"fwrite error");
			return;
		}
	}
}

int fs_loadnode(FILE *fd) {
	uint8_t unodebuff[4+1+2+4+4+4+4+4+4+8+4+2+8*65536+4*65536+4];
	const uint8_t *ptr,*chptr;
	uint8_t type;
	uint32_t i,indx,pleng,ch,sessionids,sessionid;
	fsnode *p;
	sessionidrec *sessionidptr;
	uint32_t nodepos;
#ifndef METARESTORE
	statsrecord *sr;
#endif
	static uint8_t nl;

	if (fd==NULL) {
		nl=1;
		return 0;
	}

	type = fgetc(fd);
	if (type==0) {  // last node
		return 1;
	}
	p = new fsnode();
	passert(p);
	p->type = type;
	switch (type) {
	case TYPE_DIRECTORY:
	case TYPE_FIFO:
	case TYPE_SOCKET:
		if (fread(unodebuff,1,4+1+2+4+4+4+4+4+4,fd)!=4+1+2+4+4+4+4+4+4) {
			int err = errno;
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			errno = err;
			mfs_errlog(LOG_ERR,"loading node: read error");
			delete p;
			return -1;
		}
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
	case TYPE_SYMLINK:
		if (fread(unodebuff,1,4+1+2+4+4+4+4+4+4+4,fd)!=4+1+2+4+4+4+4+4+4+4) {
			int err = errno;
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			errno = err;
			mfs_errlog(LOG_ERR,"loading node: read error");
			delete p;
			return -1;
		}
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		if (fread(unodebuff,1,4+1+2+4+4+4+4+4+4+8+4+2,fd)!=4+1+2+4+4+4+4+4+4+8+4+2) {
			int err = errno;
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			errno = err;
			mfs_errlog(LOG_ERR,"loading node: read error");
			delete p;
			return -1;
		}
		break;
	default:
		if (nl) {
			fputc('\n',stderr);
			nl=0;
		}
		mfs_arg_syslog(LOG_ERR,"loading node: unrecognized node type: %c",type);
		delete p;
		return -1;
	}
	ptr = unodebuff;
	p->id = get32bit(&ptr);
	p->goal = get8bit(&ptr);
	p->mode = get16bit(&ptr);
	p->uid = get32bit(&ptr);
	p->gid = get32bit(&ptr);
	p->atime = get32bit(&ptr);
	p->mtime = get32bit(&ptr);
	p->ctime = get32bit(&ptr);
	p->trashtime = get32bit(&ptr);
	switch (type) {
	case TYPE_DIRECTORY:
#ifndef METARESTORE
		sr = (statsrecord*) malloc(sizeof(statsrecord));
		passert(sr);
		memset(sr,0,sizeof(statsrecord));
		p->data.ddata.stats = sr;
#endif
		p->data.ddata.children = NULL;
		p->data.ddata.nlink = 2;
		p->data.ddata.elements = 0;
		break;
	case TYPE_SOCKET:
	case TYPE_FIFO:
/*
#ifdef CACHENOTIFY
		p->data.odata.lastattrchange=0;
#endif
*/
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		p->data.devdata.rdev = get32bit(&ptr);
/*
#ifdef CACHENOTIFY
		p->data.devdata.lastattrchange=0;
#endif
*/
		break;
	case TYPE_SYMLINK:
		pleng = get32bit(&ptr);
		p->data.sdata.pleng = pleng;
		if (pleng>0) {
			p->data.sdata.path = (uint8_t*) malloc(pleng);
			passert(p->data.sdata.path);
			if (fread(p->data.sdata.path,1,pleng,fd)!=pleng) {
				int err = errno;
				if (nl) {
					fputc('\n',stderr);
					nl=0;
				}
				errno = err;
				mfs_errlog(LOG_ERR,"loading node: read error");
				free(p->data.sdata.path);
				delete p;
				return -1;
			}
		} else {
			p->data.sdata.path = NULL;
		}
/*
#ifdef CACHENOTIFY
		p->data.sdata.lastattrchange=0;
#endif
*/
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		p->data.fdata.length = get64bit(&ptr);
		ch = get32bit(&ptr);
		p->data.fdata.chunks = ch;
		sessionids = get16bit(&ptr);
		if (ch>0) {
			p->data.fdata.chunktab = (uint64_t*) malloc(sizeof(uint64_t)*ch);
			passert(p->data.fdata.chunktab);
		} else {
			p->data.fdata.chunktab = NULL;
		}
		indx = 0;
		while (ch>65536) {
			chptr = ptr;
			if (fread((uint8_t*)ptr,1,8*65536,fd)!=8*65536) {
				int err = errno;
				if (nl) {
					fputc('\n',stderr);
					nl=0;
				}
				errno = err;
				mfs_errlog(LOG_ERR,"loading node: read error");
				if (p->data.fdata.chunktab) {
					free(p->data.fdata.chunktab);
				}
				delete p;
				return -1;
			}
			for (i=0 ; i<65536 ; i++) {
				p->data.fdata.chunktab[indx] = get64bit(&chptr);
				indx++;
			}
			ch-=65536;
		}
		if (fread((uint8_t*)ptr,1,8*ch+4*sessionids,fd)!=8*ch+4*sessionids) {
			int err = errno;
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			errno = err;
			mfs_errlog(LOG_ERR,"loading node: read error");
			if (p->data.fdata.chunktab) {
				free(p->data.fdata.chunktab);
			}
			delete p;
			return -1;
		}
		for (i=0 ; i<ch ; i++) {
			p->data.fdata.chunktab[indx] = get64bit(&ptr);
			indx++;
		}
		p->data.fdata.sessionids=NULL;
		while (sessionids) {
			sessionid = get32bit(&ptr);
			sessionidptr = sessionidrec_malloc();
			sessionidptr->sessionid = sessionid;
			sessionidptr->next = p->data.fdata.sessionids;
			p->data.fdata.sessionids = sessionidptr;
#ifndef METARESTORE
			matoclserv_init_sessions(sessionid,p->id);
#endif
			sessionids--;
		}
#ifndef METARESTORE
		fsnodes_quota_update_size(p, +fsnodes_get_size(p));
#endif
/*
#ifdef CACHENOTIFY
		p->data.fdata.lastattrchange=0;
#endif
*/
	}
	p->parents = NULL;
	nodepos = NODEHASHPOS(p->id);
	p->next = nodehash[nodepos];
	nodehash[nodepos] = p;
	fsnodes_used_inode(p->id);
	nodes++;
	if (type==TYPE_DIRECTORY) {
		dirnodes++;
	}
	if (type==TYPE_FILE || type==TYPE_TRASH || type==TYPE_RESERVED) {
		filenodes++;
	}
#ifndef METARESTORE
	fsnodes_quota_register_inode(p);
#endif
	return 0;
}

void fs_storenodes(FILE *fd) {
	uint32_t i;
	fsnode *p;
	for (i=0 ; i<NODEHASHSIZE ; i++) {
		for (p=nodehash[i] ; p ; p=p->next) {
			fs_storenode(p,fd);
		}
	}
	fs_storenode(NULL,fd);  // end marker
}

void fs_storeedgelist(fsedge *e,FILE *fd) {
	while (e) {
		fs_storeedge(e,fd);
		e=e->nextchild;
	}
}

void fs_storeedges_rec(fsnode *f,FILE *fd) {
	fsedge *e;
	fs_storeedgelist(f->data.ddata.children,fd);
	for (e=f->data.ddata.children ; e ; e=e->nextchild) {
		if (e->child->type==TYPE_DIRECTORY) {
			fs_storeedges_rec(e->child,fd);
		}
	}
}

void fs_storeedges(FILE *fd) {
	fs_storeedges_rec(root,fd);
	fs_storeedgelist(trash,fd);
	fs_storeedgelist(reserved,fd);
	fs_storeedge(NULL,fd);  // end marker
}

static void fs_storeacls(FILE *fd) {
	for (uint32_t i = 0; i < NODEHASHSIZE; ++i) {
		for (fsnode *p = nodehash[i]; p; p = p->next) {
			if (p->extendedAcl || p->defaultAcl) {
				fs_storeacl(p, fd);
			}
		}
	}
	fs_storeacl(nullptr, fd); // end marker
}

static void fs_storequotas(FILE *fd) {
	const std::vector<QuotaEntry>& entries = gQuotaDatabase.getEntries();
	fs_store_generic(fd, entries);
}

int fs_lostnode(fsnode *p) {
	uint8_t artname[40];
	uint32_t i,l;
	i=0;
	do {
		if (i==0) {
			l = snprintf((char*)artname,40,"lost_node_%" PRIu32,p->id);
		} else {
			l = snprintf((char*)artname,40,"lost_node_%" PRIu32 ".%" PRIu32,p->id,i);
		}
		if (!fsnodes_nameisused(root,l,artname)) {
			fsnodes_link(0,root,p,l,artname);
			return 1;
		}
		i++;
	} while (i);
	return -1;
}

int fs_checknodes(int ignoreflag) {
	uint32_t i;
	uint8_t nl;
	fsnode *p;
	nl=1;
	for (i=0 ; i<NODEHASHSIZE ; i++) {
		for (p=nodehash[i] ; p ; p=p->next) {
			if (p->parents==NULL && p!=root) {
				if (nl) {
					fputc('\n',stderr);
					nl=0;
				}
				fprintf(stderr,"found orphaned inode: %" PRIu32 "\n",p->id);
#ifndef METARESTORE
				syslog(LOG_ERR,"found orphaned inode: %" PRIu32,p->id);
#endif
				if (ignoreflag) {
					if (fs_lostnode(p)<0) {
						return -1;
					}
				} else {
					fprintf(stderr,"use mfsmetarestore (option -i) to attach this node to root dir\n");
					return -1;
				}
			}
		}
	}
	return 1;
}

int fs_loadnodes(FILE *fd) {
	int s;
	fs_loadnode(NULL);
	do {
		s = fs_loadnode(fd);
		if (s<0) {
			return -1;
		}
	} while (s==0);
	return 0;
}

int fs_loadedges(FILE *fd,int ignoreflag) {
	int s;
	fs_loadedge(NULL,ignoreflag);   // init
	do {
		s = fs_loadedge(fd,ignoreflag);
		if (s<0) {
			return -1;
		}
	} while (s==0);
	return 0;
}

static int fs_loadacls(FILE *fd, int ignoreflag) {
	fs_loadacl(NULL, ignoreflag); // init
	int s = 0;
	do {
		s = fs_loadacl(fd, ignoreflag);
		if (s < 0) {
			return -1;
		}
	} while (s == 0);
	return 0;
}

static int fs_loadquotas(FILE *fd, int ignoreflag) {
	try {
		std::vector<QuotaEntry> entries;
		fs_load_generic(fd, entries);
		for (const auto& entry : entries) {
			gQuotaDatabase.set(entry.entryKey.rigor, entry.entryKey.resource,
					entry.entryKey.owner.ownerType, entry.entryKey.owner.ownerId, entry.limit);
		}
	} catch (Exception& ex) {
		mfs_arg_syslog(LOG_ERR, "loading quotas: %s", ex.what());
		if (!ignoreflag || ex.status() != STATUS_OK) {
			return -1;
		}
	}
	return 0;
}

void fs_storefree(FILE *fd) {
	uint8_t wbuff[8*1024],*ptr;
	freenode *n;
	uint32_t l;
	l=0;
	for (n=freelist ; n ; n=n->next) {
		l++;
	}
	ptr = wbuff;
	put32bit(&ptr,l);
	if (fwrite(wbuff,1,4,fd)!=(size_t)4) {
		syslog(LOG_NOTICE,"fwrite error");
		return;
	}
	l=0;
	ptr=wbuff;
	for (n=freelist ; n ; n=n->next) {
		if (l==1024) {
			if (fwrite(wbuff,1,8*1024,fd)!=(size_t)(8*1024)) {
				syslog(LOG_NOTICE,"fwrite error");
				return;
			}
			l=0;
			ptr=wbuff;
		}
		put32bit(&ptr,n->id);
		put32bit(&ptr,n->ftime);
		l++;
	}
	if (l>0) {
		if (fwrite(wbuff,1,8*l,fd)!=(size_t)(8*l)) {
			syslog(LOG_NOTICE,"fwrite error");
			return;
		}
	}
}

int fs_loadfree(FILE *fd) {
	uint8_t rbuff[8*1024];
	const uint8_t *ptr;
	freenode *n;
	uint32_t l,t;
	uint8_t nl=1;

	if (fread(rbuff,1,4,fd)!=4) {
		int err = errno;
		if (nl) {
			fputc('\n',stderr);
			// nl=0;
		}
		errno = err;
		mfs_errlog(LOG_ERR,"loading free nodes: read error");
		return -1;
	}
	ptr=rbuff;
	t = get32bit(&ptr);
	freelist = NULL;
	freetail = &(freelist);
	l=0;
	while (t>0) {
		if (l==0) {
			if (t>1024) {
				if (fread(rbuff,1,8*1024,fd)!=8*1024) {
					int err = errno;
					if (nl) {
						fputc('\n',stderr);
						// nl=0;
					}
					errno = err;
					mfs_errlog(LOG_ERR,"loading free nodes: read error");
					return -1;
				}
				l=1024;
			} else {
				if (fread(rbuff,1,8*t,fd)!=8*t) {
					int err = errno;
					if (nl) {
						fputc('\n',stderr);
					}
					errno = err;
					mfs_errlog(LOG_ERR,"loading free nodes: read error");
					return -1;
				}
				l=t;
			}
			ptr = rbuff;
		}
		n = freenode_malloc();
		n->id = get32bit(&ptr);
		n->ftime = get32bit(&ptr);
		n->next = NULL;
		*freetail = n;
		freetail = &(n->next);
		fsnodes_used_inode(n->id);
		l--;
		t--;
	}
	return 0;
}

void fs_store(FILE *fd,uint8_t fver) {
	uint8_t hdr[16];
	uint8_t *ptr;
	off_t offbegin,offend;

	ptr = hdr;
	put32bit(&ptr,maxnodeid);
	put64bit(&ptr,metaversion);
	put32bit(&ptr,nextsessionid);
	if (fwrite(hdr,1,16,fd)!=(size_t)16) {
		syslog(LOG_NOTICE,"fwrite error");
		return;
	}
	if (fver >= kMetadataVersionWithSections) {
		offbegin = ftello(fd);
		fseeko(fd,offbegin+16,SEEK_SET);
	} else {
		offbegin = 0;   // makes some old compilers happy
	}
	fs_storenodes(fd);
	if (fver >= kMetadataVersionWithSections) {
		offend = ftello(fd);
		memcpy(hdr,"NODE 1.0",8);
		ptr = hdr+8;
		put64bit(&ptr,offend-offbegin-16);
		fseeko(fd,offbegin,SEEK_SET);
		if (fwrite(hdr,1,16,fd)!=(size_t)16) {
			syslog(LOG_NOTICE,"fwrite error");
			return;
		}
		offbegin = offend;
		fseeko(fd,offbegin+16,SEEK_SET);
	}
	fs_storeedges(fd);
	if (fver >= kMetadataVersionWithSections) {
		offend = ftello(fd);
		memcpy(hdr,"EDGE 1.0",8);
		ptr = hdr+8;
		put64bit(&ptr,offend-offbegin-16);
		fseeko(fd,offbegin,SEEK_SET);
		if (fwrite(hdr,1,16,fd)!=(size_t)16) {
			syslog(LOG_NOTICE,"fwrite error");
			return;
		}
		offbegin = offend;
		fseeko(fd,offbegin+16,SEEK_SET);
	}
	fs_storefree(fd);
	if (fver >= kMetadataVersionWithSections) {
		offend = ftello(fd);
		memcpy(hdr,"FREE 1.0",8);
		ptr = hdr+8;
		put64bit(&ptr,offend-offbegin-16);
		fseeko(fd,offbegin,SEEK_SET);
		if (fwrite(hdr,1,16,fd)!=(size_t)16) {
			syslog(LOG_NOTICE,"fwrite error");
			return;
		}
		offbegin = offend;
		fseeko(fd,offbegin+16,SEEK_SET);

		xattr_store(fd);

		offend = ftello(fd);
		memcpy(hdr,"XATR 1.0",8);
		ptr = hdr+8;
		put64bit(&ptr,offend-offbegin-16);
		fseeko(fd,offbegin,SEEK_SET);
		if (fwrite(hdr,1,16,fd)!=(size_t)16) {
			syslog(LOG_NOTICE,"fwrite error");
			return;
		}
		offbegin = offend;
		fseeko(fd,offbegin+16,SEEK_SET);

		fs_storeacls(fd);

		offend = ftello(fd);
		memcpy(hdr,"ACLS 1.0",8);
		ptr = hdr+8;
		put64bit(&ptr,offend-offbegin-16);
		fseeko(fd,offbegin,SEEK_SET);
		if (fwrite(hdr,1,16,fd)!=(size_t)16) {
			syslog(LOG_NOTICE,"fwrite error");
			return;
		}
		offbegin = offend;
		fseeko(fd,offbegin+16,SEEK_SET);

		fs_storequotas(fd);

		offend = ftello(fd);
		memcpy(hdr,"QUOT 1.1",8);
		ptr = hdr+8;
		put64bit(&ptr,offend-offbegin-16);
		fseeko(fd,offbegin,SEEK_SET);
		if (fwrite(hdr,1,16,fd)!=(size_t)16) {
			syslog(LOG_NOTICE,"fwrite error");
			return;
		}
		offbegin = offend;
		fseeko(fd,offbegin+16,SEEK_SET);
	}
	chunk_store(fd);
	if (fver >= kMetadataVersionWithSections) {
		offend = ftello(fd);
		memcpy(hdr,"CHNK 1.0",8);
		ptr = hdr+8;
		put64bit(&ptr,offend-offbegin-16);
		fseeko(fd,offbegin,SEEK_SET);
		if (fwrite(hdr,1,16,fd)!=(size_t)16) {
			syslog(LOG_NOTICE,"fwrite error");
			return;
		}

		fseeko(fd,offend,SEEK_SET);
		memcpy(hdr,"[MFS EOF MARKER]",16);
		if (fwrite(hdr,1,16,fd)!=(size_t)16) {
			syslog(LOG_NOTICE,"fwrite error");
			return;
		}
	}
}

static void fs_store_fd(FILE* fd) {
#if VERSHEX >= LIZARDFS_VERSION(1, 6, 29)
	const char hdr[] = MFSSIGNATURE "M 2.0";
	const uint8_t metadataVersion = kMetadataVersionWithSections;
#else
	const char hdr[] = MFSSIGNATURE "M 1.6";
	const uint8_t metadataVersion = kMetadataVersionLizardFS;
#endif

	if (fwrite(&hdr, 1, sizeof(hdr)-1, fd) != sizeof(hdr)-1) {
		syslog(LOG_NOTICE,"fwrite error");
	} else {
		fs_store(fd, metadataVersion);
	}
}

uint64_t fs_loadversion(FILE *fd) {
	uint8_t hdr[12];
	const uint8_t *ptr;
	uint64_t fversion;

	if (fread(hdr,1,12,fd)!=12) {
		return 0;
	}
	ptr = hdr+4;
	fversion = get64bit(&ptr);
	return fversion;
}

int fs_load(FILE *fd,int ignoreflag,uint8_t fver) {
	uint8_t hdr[16];
	const uint8_t *ptr;
	off_t offbegin;
	uint64_t sleng;

	if (fread(hdr,1,16,fd)!=16) {
		fprintf(stderr,"error loading header\n");
		return -1;
	}
	ptr = hdr;
	maxnodeid = get32bit(&ptr);
	metaversion = get64bit(&ptr);
	nextsessionid = get32bit(&ptr);
	fsnodes_init_freebitmask();

	if (fver < kMetadataVersionWithSections) {
		fprintf(stderr,"loading objects (files,directories,etc.) ... ");
		fflush(stderr);
		if (fs_loadnodes(fd)<0) {
#ifndef METARESTORE
			syslog(LOG_ERR,"error reading metadata (node)");
#endif
			return -1;
		}
		fprintf(stderr,"ok\n");
		fprintf(stderr,"loading names ... ");
		fflush(stderr);
		if (fs_loadedges(fd,ignoreflag)<0) {
#ifndef METARESTORE
			syslog(LOG_ERR,"error reading metadata (edge)");
#endif
			return -1;
		}
		fprintf(stderr,"ok\n");
		fprintf(stderr,"loading deletion timestamps ... ");
		fflush(stderr);
		if (fs_loadfree(fd)<0) {
#ifndef METARESTORE
			syslog(LOG_ERR,"error reading metadata (free)");
#endif
			return -1;
		}
		fprintf(stderr,"ok\n");
		fprintf(stderr,"loading chunks data ... ");
		fflush(stderr);
		bool loadLockIds = (fver == kMetadataVersionLizardFS);
		if (chunk_load(fd, loadLockIds)<0) {
			fprintf(stderr,"error\n");
#ifndef METARESTORE
			syslog(LOG_ERR,"error reading metadata (chunks)");
#endif
			fclose(fd);
			return -1;
		}
		fprintf(stderr,"ok\n");
	} else { // metadata with sections
		while (1) {
			if (fread(hdr,1,16,fd)!=16) {
				fprintf(stderr,"error section header\n");
				return -1;
			}
			if (memcmp(hdr,"[MFS EOF MARKER]",16)==0) {
				break;
			}
			ptr = hdr+8;
			sleng = get64bit(&ptr);
			offbegin = ftello(fd);
			if (memcmp(hdr,"NODE 1.0",8)==0) {
				fprintf(stderr,"loading objects (files,directories,etc.) ... ");
				fflush(stderr);
				if (fs_loadnodes(fd)<0) {
#ifndef METARESTORE
					syslog(LOG_ERR,"error reading metadata (node)");
#endif
					return -1;
				}
			} else if (memcmp(hdr,"EDGE 1.0",8)==0) {
				fprintf(stderr,"loading names ... ");
				fflush(stderr);
				if (fs_loadedges(fd,ignoreflag)<0) {
#ifndef METARESTORE
					syslog(LOG_ERR,"error reading metadata (edge)");
#endif
					return -1;
				}
			} else if (memcmp(hdr,"FREE 1.0",8)==0) {
				fprintf(stderr,"loading deletion timestamps ... ");
				fflush(stderr);
				if (fs_loadfree(fd)<0) {
#ifndef METARESTORE
					syslog(LOG_ERR,"error reading metadata (free)");
#endif
					return -1;
				}
			} else if (memcmp(hdr,"XATR 1.0",8)==0) {
				fprintf(stderr,"loading extra attributes (xattr) ... ");
				fflush(stderr);
				if (xattr_load(fd,ignoreflag)<0) {
#ifndef METARESTORE
					syslog(LOG_ERR,"error reading metadata (xattr)");
#endif
					return -1;
				}
			} else if (memcmp(hdr,"ACLS 1.0",8)==0) {
				fprintf(stderr,"loading access control lists ... ");
				fflush(stderr);
				if (fs_loadacls(fd, ignoreflag)<0) {
#ifndef METARESTORE
					syslog(LOG_ERR,"error reading access control lists");
#endif
					return -1;
				}
			} else if (memcmp(hdr,"QUOT 1.0",8)==0) {
				fprintf(stderr,"old quota entries found, ignoring ... ");
				fseeko(fd,sleng,SEEK_CUR);
			} else if (memcmp(hdr,"QUOT 1.1",8)==0) {
				fprintf(stderr,"loading quota entries ... ");
				fflush(stderr);
				if (fs_loadquotas(fd, ignoreflag)<0) {
#ifndef METARESTORE
					syslog(LOG_ERR,"error reading quota entries");
#endif
					return -1;
				}
			} else if (memcmp(hdr,"LOCK 1.0",8)==0) {
				fprintf(stderr,"ignoring locks\n");
				fseeko(fd,sleng,SEEK_CUR);
			} else if (memcmp(hdr,"CHNK 1.0",8)==0) {
				fprintf(stderr,"loading chunks data ... ");
				fflush(stderr);
				if (chunk_load(fd, true)<0) {
					fprintf(stderr,"error\n");
#ifndef METARESTORE
					syslog(LOG_ERR,"error reading metadata (chunks)");
#endif
					fclose(fd);
					return -1;
				}
			} else {
				hdr[8]=0;
				if (ignoreflag) {
					fprintf(stderr,"unknown section found (leng:%" PRIu64 ",name:%s) - all data from this section will be lost !!!\n",sleng,hdr);
					fseeko(fd,sleng,SEEK_CUR);
				} else {
					fprintf(stderr,"error: unknown section found (leng:%" PRIu64 ",name:%s)\n",sleng,hdr);
					return -1;
				}
			}
			if ((off_t)(offbegin+sleng)!=ftello(fd)) {
				fprintf(stderr,"not all section has been read - file corrupted\n");
				if (ignoreflag==0) {
					return -1;
				}
			}
			fprintf(stderr,"ok\n");
		}
	}

	fprintf(stderr,"checking filesystem consistency ... ");
	fflush(stderr);
	root = fsnodes_id_to_node(MFS_ROOT_ID);
	if (root==NULL) {
		fprintf(stderr,"root node not found !!!\n");
#ifndef METARESTORE
		syslog(LOG_ERR,"error reading metadata (no root)");
#endif
		return -1;
	}
	if (fs_checknodes(ignoreflag)<0) {
		return -1;
	}
	fprintf(stderr,"ok\n");
	return 0;
}

#ifndef METARESTORE
void fs_new(void) {
	uint32_t nodepos;
	statsrecord *sr;
	maxnodeid = MFS_ROOT_ID;
	metaversion = 0;
	nextsessionid = 1;
	fsnodes_init_freebitmask();
	freelist = NULL;
	freetail = &(freelist);
	root = new fsnode();
	passert(root);
	root->id = MFS_ROOT_ID;
	root->type = TYPE_DIRECTORY;
	root->ctime = root->mtime = root->atime = main_time();
	root->goal = DEFAULT_GOAL;
	root->trashtime = DEFAULT_TRASHTIME;
	root->mode = 0777;
	root->uid = 0;
	root->gid = 0;
	sr = (statsrecord*) malloc(sizeof(statsrecord));
	passert(sr);
	memset(sr,0,sizeof(statsrecord));
	root->data.ddata.stats = sr;
	root->data.ddata.children = NULL;
	root->data.ddata.elements = 0;
	root->data.ddata.nlink = 2;
	root->parents = NULL;
	nodepos = NODEHASHPOS(root->id);
	root->next = nodehash[nodepos];
	nodehash[nodepos] = root;
	fsnodes_used_inode(root->id);
	chunk_newfs();
	nodes=1;
	dirnodes=1;
	filenodes=0;
	fs_checksum(ChecksumMode::kForceRecalculate);
	fsnodes_quota_register_inode(root);
}
#endif

int fs_emergency_storeall(const char *fname) {
	FILE *fd;
	fd = fopen(fname,"w");
	if (fd==NULL) {
		return -1;
	}

	fs_store_fd(fd);

	if (ferror(fd)!=0) {
		fclose(fd);
		return -1;
	}
	fclose(fd);
	syslog(LOG_WARNING,"metadata were stored to emergency file: %s - please copy this file to your default location as '" METADATA_FILENAME "'",fname);
	return 0;
}

int fs_emergency_saves() {
#if defined(HAVE_PWD_H) && defined(HAVE_GETPWUID)
	struct passwd *p;
#endif
	if (fs_emergency_storeall(METADATA_EMERGENCY_FILENAME)==0) {
		return 0;
	}
#if defined(HAVE_PWD_H) && defined(HAVE_GETPWUID)
	p = getpwuid(getuid());
	if (p) {
		char *fname;
		int l;
		l = strlen(p->pw_dir);
		fname = malloc(l+24);
		if (fname) {
			memcpy(fname,p->pw_dir,l);
			fname[l]='/';
			memcpy(fname+l+1,METADATA_EMERGENCY_FILENAME,22);
			fname[l+23]=0;
			if (fs_emergency_storeall(fname)==0) {
				free(fname);
				return 0;
			}
			free(fname);
		}
	}
#endif
	if (fs_emergency_storeall("/" METADATA_EMERGENCY_FILENAME)==0) {
		return 0;
	}
	if (fs_emergency_storeall("/tmp/" METADATA_EMERGENCY_FILENAME)==0) {
		return 0;
	}
	if (fs_emergency_storeall("/var/" METADATA_EMERGENCY_FILENAME)==0) {
		return 0;
	}
	if (fs_emergency_storeall("/usr/" METADATA_EMERGENCY_FILENAME)==0) {
		return 0;
	}
	if (fs_emergency_storeall("/usr/share/" METADATA_EMERGENCY_FILENAME)==0) {
		return 0;
	}
	if (fs_emergency_storeall("/usr/local/" METADATA_EMERGENCY_FILENAME)==0) {
		return 0;
	}
	if (fs_emergency_storeall("/usr/local/var/" METADATA_EMERGENCY_FILENAME)==0) {
		return 0;
	}
	if (fs_emergency_storeall("/usr/local/share/" METADATA_EMERGENCY_FILENAME)==0) {
		return 0;
	}
	return -1;
}

#ifndef METARESTORE
// returns false in case of an error
bool fs_storeall(MetadataDumper::DumpType dumpType) {
	FILE *fd;

	if (metadataDumper.inProgress()) {
		syslog(LOG_ERR, "previous metadata save process hasn't finished yet - do not start another one");
		return false;
	}
	changelog_rotate();
	// child == true says that we forked
	// bg may be changed to dump in foreground in case of a fork error
	bool child = metadataDumper.start(dumpType, fs_checksum(ChecksumMode::kGetCurrent));

	if (dumpType == MetadataDumper::kForegroundDump) {
		fd = fopen(METADATA_BACK_TMP_FILENAME, "w");
		if (fd == NULL) {
			syslog(LOG_ERR, "can't open metadata file");
			// try to save in alternative location - just in case
			fs_emergency_saves();
			if (child) {
				exit(1);
			}
			return false;
		}

		fs_store_fd(fd);

		if (ferror(fd) != 0) {
			syslog(LOG_ERR, "can't write metadata");
			fclose(fd);
			unlink(METADATA_BACK_TMP_FILENAME);
			// try to save in alternative location - just in case
			fs_emergency_saves();
			if (child) {
				exit(1);
			}
			return false;
		} else {
			if (fflush(fd) == EOF) {
				mfs_errlog(LOG_ERR, "metadata fflush failed");
			} else if (fsync(fileno(fd)) == -1) {
				mfs_errlog(LOG_ERR, "metadata fsync failed");
			}
			fclose(fd);
			if (!child) {
				// rename backups if no child was created, otherwise this is handled by pollServe
				renameBackupFiles();
			}
		}
		if (child) {
			printf("OK\n"); // give mfsmetarestore another chance
			exit(0);
		}
	}
	sassert(!child);
	return true;
}

void fs_dostoreall() {
	fs_storeall(MetadataDumper::kBackgroundDump); // ignore error
}

void fs_term(void) {
	for (;;) {
		if (fs_storeall(MetadataDumper::kForegroundDump)) {
			// store was successful
			if (rename(METADATA_BACK_FILENAME, METADATA_FILENAME) < 0) {
				mfs_errlog(LOG_WARNING, "can't rename " METADATA_BACK_FILENAME " -> " METADATA_FILENAME);
			}
			chunk_term();
			return ;
		}
		syslog(LOG_ERR,"can't store metadata - try to make more space on your hdd or change privieleges - retrying after 10 seconds");
		sleep(10);
	}
}

#else
void fs_storeall(const char *fname) {
	FILE *fd;
	fd = fopen(fname,"w");
	if (fd==NULL) {
		fprintf(stderr, "can't open metadata file\n");
		return;
	}
	fs_store_fd(fd);

	if (ferror(fd)!=0) {
		fprintf(stderr, "can't write metadata\n");
	} else if (fflush(fd) == EOF) {
		fprintf(stderr, "can't fflush metadata\n");
	} else if (fsync(fileno(fd)) == -1) {
		fprintf(stderr, "can't fsync metadata\n");
	}
	fclose(fd);
}

void fs_term(const char *fname) {
	fs_storeall(fname);
}
#endif

LIZARDFS_CREATE_EXCEPTION_CLASS(MetadataException, Exception);
LIZARDFS_CREATE_EXCEPTION_CLASS(MetadataFSConsistencyException, MetadataException);
LIZARDFS_CREATE_EXCEPTION_CLASS(MetadataConsistencyException, MetadataException);
char const BackupNewerThanCurrentMsg[] =
	"backup file is newer than current file - please check"
	" it manually - propably you should run metarestore";
char const RenameCurrentToBackupMsg[] =
	"can't rename " METADATA_FILENAME " -> " METADATA_BACK_FILENAME;
char const MetadataStructureReadErrorMsg[] = "error reading metadata (structure)";

#ifndef METARESTORE
void fs_loadall(void) {
#else
void fs_loadall(const char *fname,int ignoreflag) {
#endif
	uint8_t hdr[8];
#ifndef METARESTORE
	uint8_t bhdr[8];
	uint64_t backversion;
#endif

#ifdef METARESTORE
	cstream_t fd(fopen(fname,"r"));
#else
	backversion = 0;
	cstream_t fd(fopen(METADATA_BACK_FILENAME,"r"));
	if (fd != nullptr) {
		if (fread(bhdr,1,8,fd.get())==8) {
			// bhdr is something like "MFSM x.y" or "LFSM x.y" (for Light LizardsFS)
			std::string sig(reinterpret_cast<const char *>(bhdr), 5);
			std::string ver(reinterpret_cast<const char *>(bhdr) + 5, 3);
			if (sig == MFSSIGNATURE "M " && (ver == "1.5" || ver == "1.6" || ver == "2.0")) {
				backversion = fs_loadversion(fd.get());
			}
		}
	}

	fd.reset(fopen(METADATA_FILENAME,"r"));
#endif
	if (fd == nullptr) {
		int savedErrno(errno);
#ifndef METARESTORE
		{
#if defined(HAVE_GETCWD)
#ifndef PATH_MAX
#define PATH_MAX 10000
#endif
			char cwdbuf[PATH_MAX+1];
			int cwdlen;
			if (getcwd(cwdbuf,PATH_MAX)==NULL) {
				cwdbuf[0]=0;
			} else {
				cwdlen = strlen(cwdbuf);
				if (cwdlen>0 && cwdlen<PATH_MAX-1 && cwdbuf[cwdlen-1]!='/') {
					cwdbuf[cwdlen]='/';
					cwdbuf[cwdlen+1]=0;
				} else {
					cwdbuf[0]=0;
				}
			}

#else
			char cwdbuf[1];
			cwdbuf[0]=0;
#endif
			fprintf(stderr, "Can't open metadata file: If this is new instalation "
				"then rename %s" METADATA_FILENAME ".empty "
				"as %s" METADATA_FILENAME, cwdbuf, cwdbuf);
			if (!cwdbuf[0]) {
				fprintf( stderr, " (in current working directory)");
			}
			fprintf(stderr, "\n");
		}
#endif
		if (savedErrno == ENOENT)
			throw MetadataFSConsistencyException("metadata file does not exits");
		else
			throw FilesystemException("can't open metadata file");
	}
	if (fread(hdr,1,8,fd.get())!=8) {
		throw MetadataConsistencyException("can't read metadata header");
	}
#ifndef METARESTORE
	if (memcmp(hdr,"MFSM NEW",8)==0) {      // special case - create new file system
		if (backversion>0) {
			throw MetadataConsistencyException(BackupNewerThanCurrentMsg);
		}
		if (rename(METADATA_FILENAME,METADATA_BACK_FILENAME)<0) {
			throw FilesystemException(RenameCurrentToBackupMsg);
		}
		fprintf(stderr,"create new empty filesystem");
		syslog(LOG_NOTICE,"create new empty filesystem");
		fs_new();
		unlink(METADATA_BACK_TMP_FILENAME);
		// after creating new filesystem always create "back" file for using in metarestore
		fs_storeall(MetadataDumper::kForegroundDump);
		return;
	}
#endif
	uint8_t metadataVersion;
	if (memcmp(hdr,MFSSIGNATURE "M 1.5",8)==0) {
		metadataVersion = kMetadataVersionMooseFS;
	} else if (memcmp(hdr,MFSSIGNATURE "M 1.6",8)==0) {
		metadataVersion = kMetadataVersionLizardFS;
	} else if (memcmp(hdr,MFSSIGNATURE "M 2.0",8)==0) {
		metadataVersion = kMetadataVersionWithSections;
	} else {
		throw MetadataConsistencyException("wrong metadata header version");
	}
#ifndef METARESTORE
	if (fs_load(fd.get(), 0, metadataVersion) < 0) {
#else
	if (fs_load(fd.get(), ignoreflag, metadataVersion) < 0) {
#endif
		throw MetadataConsistencyException(MetadataStructureReadErrorMsg);
	}
	if (ferror(fd.get())!=0) {
		throw MetadataConsistencyException(MetadataStructureReadErrorMsg);
	}
#ifndef METARESTORE
	if (backversion>metaversion) {
		throw MetadataConsistencyException(BackupNewerThanCurrentMsg);
	}
	if (rename(METADATA_FILENAME,METADATA_BACK_FILENAME)<0) {
		throw FilesystemException(RenameCurrentToBackupMsg);
	}
#endif
	fprintf(stderr,"connecting files and chunks ... ");
	fflush(stderr);
	fs_add_files_to_chunks();
	fprintf(stderr,"ok\n");
#ifndef METARESTORE
	fprintf(stderr,"all inodes: %" PRIu32 "\n",nodes);
	fprintf(stderr,"directory inodes: %" PRIu32 "\n",dirnodes);
	fprintf(stderr,"file inodes: %" PRIu32 "\n",filenodes);
	fprintf(stderr,"chunks: %" PRIu32 "\n",chunk_count());
#endif
	unlink(METADATA_BACK_TMP_FILENAME);
	fs_checksum(ChecksumMode::kForceRecalculate);
	return;
}

void fs_strinit(void) {
	uint32_t i;
	root = NULL;
	trash = NULL;
	reserved = NULL;
	trashspace = 0;
	reservedspace = 0;
	trashnodes = 0;
	reservednodes = 0;
	xattr_init();
	for (i=0 ; i<NODEHASHSIZE ; i++) {
		nodehash[i]=NULL;
	}
#ifdef EDGEHASH
	for (i=0 ; i<EDGEHASHSIZE ; i++) {
		edgehash[i]=NULL;
	}
#endif
}

#ifndef METARESTORE

void fs_cs_disconnected(void) {
	test_start_time = main_time()+600;
}

void fs_reload(void) {
	gStoredPreviousBackMetaCopies = cfg_get_maxvalue(
			"BACK_META_KEEP_PREVIOUS",
			kDefaultStoredPreviousBackMetaCopies,
			kMaxStoredPreviousBackMetaCopies);
	metadataDumper.setMetarestorePath(
			cfg_get("MFSMETARESTORE_PATH", std::string(SBIN_PATH "/mfsmetarestore")));
	metadataDumper.setUseMetarestore(cfg_getint32("PREFER_BACKGROUND_DUMP", 0));
	if (cfg_getuint32("DUMP_METADATA_ON_RELOAD", 0) == 1) {
		fs_dostoreall();
	}
}

int fs_init(void) {
	fprintf(stderr,"loading metadata ...\n");
	fs_strinit();
	chunk_strinit();
	test_start_time = main_time()+900;
	try {
		fs_loadall();
	} catch(Exception const& e) {
		fprintf(stderr, "%s\n", e.what());
		syslog(LOG_ERR, "%s", e.what());
		return -1;
	}

	fprintf(stderr,"metadata file has been loaded\n");
	gStoredPreviousBackMetaCopies = cfg_get_maxvalue(
			"BACK_META_KEEP_PREVIOUS",
			kDefaultStoredPreviousBackMetaCopies,
			kMaxStoredPreviousBackMetaCopies);

	metadataDumper.setMetarestorePath(cfg_get("MFSMETARESTORE_PATH",
			std::string(SBIN_PATH "/mfsmetarestore")));
	metadataDumper.setUseMetarestore(cfg_getint32("PREFER_BACKGROUND_DUMP", 0));

	main_reloadregister(fs_reload);
	main_timeregister(TIMEMODE_RUN_LATE,1,0,fs_test_files);
	main_timeregister(TIMEMODE_RUN_LATE,3600,0,fs_dostoreall);
	main_timeregister(TIMEMODE_RUN_LATE,300,0,fs_emptytrash);
	main_timeregister(TIMEMODE_RUN_LATE,60,0,fs_emptyreserved);
	main_timeregister(TIMEMODE_RUN_LATE,60,0,fsnodes_freeinodes);
	main_pollregister(metadataPollDesc, metadataPollServe);
	main_destructregister(fs_term);
	return 0;
}
#else
int fs_init(const char *fname,int ignoreflag) {
	fs_strinit();
	chunk_strinit();
	try {
		fs_loadall(fname,ignoreflag);
	} catch (Exception const& e) {
		fprintf(stderr, "%s\n", e.what());
		return -1;
	}
	return 0;
}
#endif
