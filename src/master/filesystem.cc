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
#include "master/filesystem.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <syslog.h>
#include <unistd.h>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <boost/filesystem.hpp>

#include "common/cwrap.h"
#include "common/datapack.h"
#include "common/exceptions.h"
#include "common/hashfn.h"
#include "common/lizardfs_version.h"
#include "common/massert.h"
#include "common/metadata.h"
#include "common/MFSCommunication.h"
#include "common/rotate_files.h"
#include "common/setup.h"
#include "common/slogger.h"
#include "master/checksum.h"
#include "master/chunks.h"
#include "master/metadata_dumper.h"
#include "master/personality.h"
#include "master/quota_database.h"
#include "metarestore/restore.h"

#ifdef LIZARDFS_HAVE_PWD_H
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

#define NODEHASHBITS (22)
#define NODEHASHSIZE (1<<NODEHASHBITS)
#define NODEHASHPOS(nodeid) ((nodeid)&(NODEHASHSIZE-1))

#define EDGEHASHBITS (22)
#define EDGEHASHSIZE (1<<EDGEHASHBITS)
#define EDGEHASHPOS(hash) ((hash)&(EDGEHASHSIZE-1))
#define LOOKUPNOHASHLIMIT 10

#define XATTR_INODE_HASH_SIZE 65536
#define XATTR_DATA_HASH_SIZE 524288

#define DEFAULT_GOAL 1
#define DEFAULT_TRASHTIME 86400

#define MAXFNAMELENG 255

#define MAX_INDEX 0x7FFFFFFF

#define CHIDS_NO 0
#define CHIDS_YES 1
#define CHIDS_AUTO 2

enum class AclInheritance {
	kInheritAcl,
	kDontInheritAcl
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

struct fsedge {
	fsnode *child,*parent;
	struct fsedge *nextchild,*nextparent;
	struct fsedge **prevchild,**prevparent;
	struct fsedge *next,**prev;
	uint64_t checksum;
	uint16_t nleng;
	uint8_t *name;

	fsedge() : name(nullptr) {}

	~fsedge() {
		free(name);
	}
};
void free(fsedge*); // disable freeing using free at link time :)

typedef struct _statsrecord {
	uint32_t inodes;
	uint32_t dirs;
	uint32_t files;
	uint32_t chunks;
	uint64_t length;
	uint64_t size;
	uint64_t realsize;
} statsrecord;

#ifdef METARESTORE
void changelog(int, char const*, ...) {
	mabort("Bad code path - changelog() shall not be executed in metarestore context.");
}
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

	xattr_data_entry() : attrname(nullptr), attrvalue(nullptr) {}

	~xattr_data_entry() {
		free(attrname);
		free(attrvalue);
	}
};
void free(xattr_data_entry*); // disable freeing using free at link time :)

struct xattr_inode_entry {
	uint32_t inode;
	uint32_t anleng;
	uint32_t avleng;
	struct xattr_data_entry *data_head;
	struct xattr_inode_entry *next;
};

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
			statsrecord *stats;
		} ddata;
		struct _sdata {                         // type==TYPE_SYMLINK
			uint32_t pleng;
			uint8_t *path;
		} sdata;
		struct _devdata {
			uint32_t rdev;                          // type==TYPE_BLOCKDEV ; type==TYPE_CHARDEV
		} devdata;
		struct _fdata {                         // type==TYPE_FILE ; type==TYPE_TRASH ; type==TYPE_RESERVED
			uint64_t length;
			uint64_t *chunktab;
			uint32_t chunks;
			sessionidrec *sessionids;
		} fdata;
	} data;
	fsedge *parents;
	fsnode *next;
	uint64_t checksum;

	fsnode(uint8_t type) : type(type) {
		if (type == TYPE_DIRECTORY) {
			data.ddata.stats = nullptr;
		} else if (type == TYPE_SYMLINK) {
			data.sdata.path = nullptr;
		} else if (type == TYPE_FILE || type == TYPE_RESERVED || type == TYPE_TRASH) {
			data.fdata.chunktab = nullptr;
		}
	}

	~fsnode() {
		if (type == TYPE_DIRECTORY) {
			free(data.ddata.stats);
		} else if (type == TYPE_SYMLINK) {
			free(data.sdata.path);
		} else if (type == TYPE_FILE || type == TYPE_RESERVED || type == TYPE_TRASH) {
			free(data.fdata.chunktab);
		}
	}
};
void free(fsnode*); // disable freeing using free at link time :)

typedef struct _freenode {
	uint32_t id;
	uint32_t ftime;
	struct _freenode *next;
} freenode;

#define FREENODE_BUCKET_SIZE 5000
typedef struct _freenode_bucket {
	freenode bucket[FREENODE_BUCKET_SIZE];
	uint32_t firstfree;
	struct _freenode_bucket *next;
} freenode_bucket;

#define CUIDREC_BUCKET_SIZE 1000
typedef struct _sessionidrec_bucket {
	sessionidrec bucket[CUIDREC_BUCKET_SIZE];
	uint32_t firstfree;
	struct _sessionidrec_bucket *next;
} sessionidrec_bucket;

namespace {
/** Metadata of the filesystem.
 *  All the static variables managed by function in this file which form metadata of the filesystem.
 */
struct FilesystemMetadata {
public:
	xattr_inode_entry* xattr_inode_hash[XATTR_INODE_HASH_SIZE];
	xattr_data_entry* xattr_data_hash[XATTR_DATA_HASH_SIZE];
	uint32_t *freebitmask;
	uint32_t bitmasksize;
	uint32_t searchpos;
	freenode *freelist,**freetail;
	fsedge *trash;
	fsedge *reserved;
	fsnode *root;
	fsnode* nodehash[NODEHASHSIZE];
	fsedge* edgehash[EDGEHASHSIZE];
	freenode_bucket *fnbhead;
	freenode *fnfreehead;
	sessionidrec_bucket *crbhead;
	sessionidrec *crfreehead;

	uint32_t maxnodeid;
	uint32_t nextsessionid;
	uint32_t nodes;
	uint64_t metaversion;
	uint64_t trashspace;
	uint64_t reservedspace;
	uint32_t trashnodes;
	uint32_t reservednodes;
	uint32_t filenodes;
	uint32_t dirnodes;

	QuotaDatabase gQuotaDatabase;

	uint64_t gFsNodesChecksum;
	uint64_t gFsEdgesChecksum;
	uint64_t gXattrChecksum;

	FilesystemMetadata()
			: xattr_inode_hash{},
			  xattr_data_hash{},
			  freebitmask{},
			  bitmasksize{},
			  searchpos{},
			  freelist{},
			  freetail{},
			  trash{},
			  reserved{},
			  root{},
			  nodehash{},
			  edgehash{},
			  fnbhead{},
			  fnfreehead{},
			  crbhead{},
			  crfreehead{},
			  maxnodeid{},
			  nextsessionid{},
			  nodes{},
			  metaversion{},
			  trashspace{},
			  reservedspace{},
			  trashnodes{},
			  reservednodes{},
			  filenodes{},
			  dirnodes{},
			  gQuotaDatabase{},
			  gFsNodesChecksum{},
			  gFsEdgesChecksum{},
			  gXattrChecksum{} {
	}

	~FilesystemMetadata() {
		// Free memory allocated in xattr_inode_hash hashmap
		for (uint32_t i = 0; i < XATTR_INODE_HASH_SIZE; ++i) {
			freeListConnectedUsingNext(xattr_inode_hash[i]);
		}

		// Free memory allocated in xattr_data_hash hashmap
		for (uint32_t i = 0; i < XATTR_DATA_HASH_SIZE; ++i) {
			deleteListConnectedUsingNext(xattr_data_hash[i]);
		}

		// Free memory allocated in freebitmask
		free(freebitmask);

		// Free memory allocated in trash list
		while (trash != nullptr) {
			fsedge* next = trash->nextchild;
			delete trash;
			trash = next;
		}

		// Free memory allocated in reserved list
		while (reserved != nullptr) {
			fsedge* next = reserved->nextchild;
			delete reserved;
			reserved = next;
		}

		// Free memory allocated in nodehash hashmap
		for (uint32_t i = 0; i < NODEHASHSIZE; ++i) {
			deleteListConnectedUsingNext(nodehash[i]);
		}

		// Free memory allocated in edgehash hashmap
		for (uint32_t i = 0; i < EDGEHASHSIZE; ++i) {
			deleteListConnectedUsingNext(edgehash[i]);
		}

		// Free memory allocated in fnbhead, crbhead lists
		freeListConnectedUsingNext(fnbhead);
		freeListConnectedUsingNext(crbhead);
	}

private:
	// Frees a C-style list with elements connected using e->next pointer and allocated using malloc
	template <typename T>
	void freeListConnectedUsingNext(T* &list) {
		while (list != nullptr) {
			T* next = list->next;
			free(list);
			list = next;
		}
	}

	// Frees a C-style list with elements connected using e->next pointer and allocated using new
	template <typename T>
	void deleteListConnectedUsingNext(T* &list) {
		while (list != nullptr) {
			T* next = list->next;
			delete list;
			list = next;
		}
	}
};
} // anonymous namespace

static FilesystemMetadata* gMetadata = nullptr;

#ifndef METARESTORE

static void* gEmptyTrashHook;
static void* gEmptyReservedHook;
static void* gFreeInodesHook;
static bool gAutoRecovery = false;

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

#endif // ifndef METARESTORE

// Number of changelog file versions
uint32_t gStoredPreviousBackMetaCopies;

// Adds an entry to a changelog, updates filesystem.cc internal structures, prepends a
// proper timestamp to changelog entry
template <typename... Args>
void fs_changelog(uint32_t ts, const char* format, Args&&... args) {
	std::string tmp = "%" PRIu32 "|";
	tmp += format;
	changelog(gMetadata->metaversion++, tmp.c_str(), ts, args...);
}

/*! \brief Periodically adds CHECKSUM changelog entry.
 *  Entry is added during destruction, so that it will be generated after end of function
 *  where ChecksumUpdater is created.
 */
class ChecksumUpdater {
public:
	ChecksumUpdater(uint32_t ts)
			: ts_(ts) {
	}
	virtual ~ChecksumUpdater() {
		if (gMetadata->metaversion > lastEntry_ + period_) {
			lastEntry_ = gMetadata->metaversion;
#ifndef METARESTORE
			if (metadataserver::isMaster()) {
				std::string versionString = lizardfsVersionToString(LIZARDFS_VERSHEX);
				uint64_t checksum = fs_checksum(ChecksumMode::kGetCurrent);
				fs_changelog(ts_, "CHECKSUM(%s):%" PRIu64, versionString.c_str(), checksum);
				return;
			}
#endif /* #ifndef METARESTORE */
		}
	}
	static void setPeriod(uint32_t period) {
		period_ = period;
	}
private:
	uint32_t ts_;
	static uint32_t period_;
	static uint32_t lastEntry_;
};

uint32_t ChecksumUpdater::period_;
uint32_t ChecksumUpdater::lastEntry_ = 0;

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
	removeFromChecksum(gMetadata->gFsNodesChecksum, node->checksum);
	node->checksum = fsnodes_checksum(node);
	addToChecksum(gMetadata->gFsNodesChecksum, node->checksum);
}

static void fsnodes_recalculate_checksum() {
	gMetadata->gFsNodesChecksum = 12345; // arbitrary number
	// nodes
	for (uint32_t i = 0; i < NODEHASHSIZE; i++) {
		for (fsnode* node = gMetadata->nodehash[i]; node; node = node->next) {
			node->checksum = fsnodes_checksum(node);
			addToChecksum(gMetadata->gFsNodesChecksum, node->checksum);
		}
	}
}

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
	removeFromChecksum(gMetadata->gFsEdgesChecksum, edge->checksum);
	edge->checksum = fsedges_checksum(edge);
	addToChecksum(gMetadata->gFsEdgesChecksum, edge->checksum);
}

static void fsedges_recalculate_checksum() {
	gMetadata->gFsEdgesChecksum = 1231241261;
	// edges
	if (gMetadata->root) {
		fsedges_checksum_edges_rec(gMetadata->gFsEdgesChecksum, gMetadata->root);
	}
	if (gMetadata->trash) {
		fsedges_checksum_edges_list(gMetadata->gFsEdgesChecksum, gMetadata->trash);
	}
	if (gMetadata->reserved) {
		fsedges_checksum_edges_list(gMetadata->gFsEdgesChecksum, gMetadata->reserved);
	}
}

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
	removeFromChecksum(gMetadata->gXattrChecksum, xde->checksum);
	xde->checksum = xattr_checksum(xde);
	addToChecksum(gMetadata->gXattrChecksum, xde->checksum);
}

static void xattr_recalculate_checksum() {
	gMetadata->gXattrChecksum = 29857986791741783ULL;
	for (int i = 0; i < XATTR_DATA_HASH_SIZE; ++i) {
		for (xattr_data_entry* xde = gMetadata->xattr_data_hash[i]; xde; xde = xde->next) {
			xde->checksum = xattr_checksum(xde);
			addToChecksum(gMetadata->gXattrChecksum, xde->checksum);
		}
	}
}

uint64_t fs_checksum(ChecksumMode mode) {
	uint64_t checksum = 0x1251;
	addToChecksum(checksum, gMetadata->maxnodeid);
	addToChecksum(checksum, gMetadata->metaversion);
	addToChecksum(checksum, gMetadata->nextsessionid);
	if (mode == ChecksumMode::kForceRecalculate) {
		fsnodes_recalculate_checksum();
		fsedges_recalculate_checksum();
		xattr_recalculate_checksum();
	}
	addToChecksum(checksum, gMetadata->gFsNodesChecksum);
	addToChecksum(checksum, gMetadata->gFsEdgesChecksum);
	addToChecksum(checksum, gMetadata->gXattrChecksum);
	addToChecksum(checksum, gMetadata->gQuotaDatabase.checksum());
	addToChecksum(checksum, chunk_checksum(mode));
	return checksum;
}

#ifndef METARESTORE
static MetadataDumper metadataDumper(kMetadataFilename, kMetadataTmpFilename);

void metadataPollDesc(struct pollfd* pdesc, uint32_t* ndesc) {
	metadataDumper.pollDesc(pdesc, ndesc);
}
void metadataPollServe(struct pollfd* pdesc) {
	bool metadataDumpInProgress = metadataDumper.inProgress();
	metadataDumper.pollServe(pdesc);
	if (metadataDumpInProgress && !metadataDumper.inProgress()) {
		if (metadataDumper.dumpSucceeded()) {
			rotateFiles(kMetadataTmpFilename, kMetadataFilename, gStoredPreviousBackMetaCopies);
		} else {
			if (metadataDumper.useMetarestore()) {
				// master should recalculate its checksum
				syslog(LOG_WARNING, "dumping metadata failed, recalculating checksum");
				fs_checksum(ChecksumMode::kForceRecalculate);
			}
			unlink(kMetadataTmpFilename);
		}
	}
}
#endif

static inline freenode* freenode_malloc() {
	freenode_bucket *fnb;
	freenode *ret;
	if (gMetadata->fnfreehead) {
		ret = gMetadata->fnfreehead;
		gMetadata->fnfreehead = ret->next;
		return ret;
	}
	if (gMetadata->fnbhead==NULL || gMetadata->fnbhead->firstfree==FREENODE_BUCKET_SIZE) {
		fnb = (freenode_bucket*)malloc(sizeof(freenode_bucket));
		passert(fnb);
		fnb->next = gMetadata->fnbhead;
		fnb->firstfree = 0;
		gMetadata->fnbhead = fnb;
	}
	ret = (gMetadata->fnbhead->bucket)+(gMetadata->fnbhead->firstfree);
	gMetadata->fnbhead->firstfree++;
	return ret;
}

static inline void freenode_free(freenode *p) {
	p->next = gMetadata->fnfreehead;
	gMetadata->fnfreehead = p;
}

static inline sessionidrec* sessionidrec_malloc() {
	sessionidrec_bucket *crb;
	sessionidrec *ret;
	if (gMetadata->crfreehead) {
		ret = gMetadata->crfreehead;
		gMetadata->crfreehead = ret->next;
		return ret;
	}
	if (gMetadata->crbhead==NULL || gMetadata->crbhead->firstfree==CUIDREC_BUCKET_SIZE) {
		crb = (sessionidrec_bucket*)malloc(sizeof(sessionidrec_bucket));
		passert(crb);
		crb->next = gMetadata->crbhead;
		crb->firstfree = 0;
		gMetadata->crbhead = crb;
	}
	ret = (gMetadata->crbhead->bucket)+(gMetadata->crbhead->firstfree);
	gMetadata->crbhead->firstfree++;
	return ret;
}

static inline void sessionidrec_free(sessionidrec *p) {
	p->next = gMetadata->crfreehead;
	gMetadata->crfreehead = p;
}

uint32_t fsnodes_get_next_id() {
	uint32_t i,mask;
	while (gMetadata->searchpos<gMetadata->bitmasksize && gMetadata->freebitmask[gMetadata->searchpos]==0xFFFFFFFF) {
		gMetadata->searchpos++;
	}
	if (gMetadata->searchpos==gMetadata->bitmasksize) {   // no more freeinodes
		uint32_t *tmpfbm;
		gMetadata->bitmasksize+=0x80;
		tmpfbm = gMetadata->freebitmask;
		gMetadata->freebitmask = (uint32_t*)realloc(gMetadata->freebitmask,gMetadata->bitmasksize*sizeof(uint32_t));
		if (gMetadata->freebitmask==NULL) {
			free(tmpfbm); // pro forma - satisfy cppcheck
		}
		passert(gMetadata->freebitmask);
		memset(gMetadata->freebitmask+gMetadata->searchpos,0,0x80*sizeof(uint32_t));
	}
	mask = gMetadata->freebitmask[gMetadata->searchpos];
	i=0;
	while (mask&1) {
		i++;
		mask>>=1;
	}
	mask = 1<<i;
	gMetadata->freebitmask[gMetadata->searchpos] |= mask;
	i+=(gMetadata->searchpos<<5);
	if (i>gMetadata->maxnodeid) {
		gMetadata->maxnodeid=i;
	}
	return i;
}

void fsnodes_free_id(uint32_t id,uint32_t ts) {
	freenode *n;
	n = freenode_malloc();
	n->id = id;
	n->ftime = ts;
	n->next = NULL;
	*gMetadata->freetail = n;
	gMetadata->freetail = &(n->next);
}

static uint32_t fs_do_freeinodes(uint32_t ts) {
	uint32_t pos,mask;
	freenode *n,*an;
	uint32_t fi = 0;
	n = gMetadata->freelist;
	while (n && n->ftime+MFS_INODE_REUSE_DELAY<ts) {
		fi++;
		pos = (n->id >> 5);
		mask = 1<<(n->id&0x1F);
		gMetadata->freebitmask[pos] &= ~mask;
		if (pos<gMetadata->searchpos) {
			gMetadata->searchpos = pos;
		}
		an = n->next;
		freenode_free(n);
		n = an;
	}
	if (n) {
		gMetadata->freelist = n;
	} else {
		gMetadata->freelist = NULL;
		gMetadata->freetail = &(gMetadata->freelist);
	}
	return fi;
}

uint8_t fs_apply_freeinodes(uint32_t ts, uint32_t freeinodes) {
	uint32_t fi = fs_do_freeinodes(ts);
	gMetadata->metaversion++;
	if (freeinodes != fi) {
		return ERROR_MISMATCH;
	}
	return 0;
}

#ifndef METARESTORE
static void fs_periodic_freeinodes(void) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	uint32_t fi = fs_do_freeinodes(ts);
	if (fi > 0) {
		fs_changelog(ts, "FREEINODES():%" PRIu32, fi);
	}
}
#endif

void fsnodes_init_freebitmask (void) {
	gMetadata->bitmasksize = 0x100+(((gMetadata->maxnodeid)>>5)&0xFFFFFF80U);
	gMetadata->freebitmask = (uint32_t*)malloc(gMetadata->bitmasksize*sizeof(uint32_t));
	passert(gMetadata->freebitmask);
	memset(gMetadata->freebitmask,0,gMetadata->bitmasksize*sizeof(uint32_t));
	gMetadata->freebitmask[0]=1;       // reserve inode 0
	gMetadata->searchpos = 0;
}

void fsnodes_used_inode (uint32_t id) {
	uint32_t pos,mask;
	pos = id>>5;
	mask = 1<<(id&0x1F);
	gMetadata->freebitmask[pos]|=mask;
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

#ifndef METARESTORE
static inline int xattr_namecheck(uint8_t anleng,const uint8_t *attrname) {
	uint32_t i;
	for (i=0 ; i<anleng ; i++) {
		if (attrname[i]=='\0') {
			return -1;
		}
	}
	return 0;
}
#endif /* METARESTORE */

static inline void xattr_removeentry(xattr_data_entry *xa) {
	*(xa->previnode) = xa->nextinode;
	if (xa->nextinode) {
		xa->nextinode->previnode = xa->previnode;
	}
	*(xa->prev) = xa->next;
	if (xa->next) {
		xa->next->prev = xa->prev;
	}
	removeFromChecksum(gMetadata->gXattrChecksum, xa->checksum);
	delete xa;
}

void xattr_removeinode(uint32_t inode) {
	xattr_inode_entry *ih,**ihp;

	ihp = &(gMetadata->xattr_inode_hash[xattr_inode_hash_fn(inode)]);
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
	for (ih = gMetadata->xattr_inode_hash[ihash]; ih && ih->inode!=inode; ih=ih->next) {}

	hash = xattr_data_hash_fn(inode,anleng,attrname);
	for (xa = gMetadata->xattr_data_hash[hash]; xa ; xa=xa->next) {
		if (xa->inode==inode && xa->anleng==anleng && memcmp(xa->attrname,attrname,anleng)==0) {
			passert(ih);
			if (mode==XATTR_SMODE_CREATE_ONLY) { // create only
				return ERROR_EEXIST;
			}
			if (mode==XATTR_SMODE_REMOVE) { // remove
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

	if (mode==XATTR_SMODE_REPLACE_ONLY || mode==XATTR_SMODE_REMOVE) {
		return ERROR_ENOATTR;
	}

	if (ih && ih->anleng+anleng+1>MFS_XATTR_LIST_MAX) {
		return ERROR_ERANGE;
	}

	xa = new xattr_data_entry;
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
	xa->next = gMetadata->xattr_data_hash[hash];
	if (xa->next) {
		xa->next->prev = &(xa->next);
	}
	xa->prev = gMetadata->xattr_data_hash + hash;
	gMetadata->xattr_data_hash[hash] = xa;

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
		ih->next = gMetadata->xattr_inode_hash[ihash];
		gMetadata->xattr_inode_hash[ihash] = ih;
	}
	xa->checksum = 0;
	xattr_update_checksum(xa);
	return STATUS_OK;
}

uint8_t xattr_getattr(uint32_t inode,uint8_t anleng,const uint8_t *attrname,uint32_t *avleng,uint8_t **attrvalue) {
	xattr_data_entry *xa;

	for (xa = gMetadata->xattr_data_hash[xattr_data_hash_fn(inode,anleng,attrname)] ; xa ; xa=xa->next) {
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
	for (ih = gMetadata->xattr_inode_hash[xattr_inode_hash_fn(inode)] ; ih ; ih=ih->next) {
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

static inline uint32_t fsnodes_hash(uint32_t parentid,uint16_t nleng,const uint8_t *name) {
	uint32_t hash,i;
	hash = ((parentid * 0x5F2318BD) + nleng);
	for (i=0 ; i<nleng ; i++) {
		hash = hash*33+name[i];
	}
	return hash;
}

static inline int fsnodes_nameisused(fsnode *node,uint16_t nleng,const uint8_t *name) {
	fsedge *ei;
	if (node->data.ddata.elements>LOOKUPNOHASHLIMIT) {
		ei = gMetadata->edgehash[EDGEHASHPOS(fsnodes_hash(node->id,nleng,name))];
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
	return 0;
}

/// searches for an edge with given name (`name`) in given directory (`node`)
static inline fsedge* fsnodes_lookup(fsnode *node,uint16_t nleng,const uint8_t *name) {
	fsedge *ei;

	if (node->type!=TYPE_DIRECTORY) {
		return NULL;
	}
	if (node->data.ddata.elements>LOOKUPNOHASHLIMIT) {
		ei = gMetadata->edgehash[EDGEHASHPOS(fsnodes_hash(node->id,nleng,name))];
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
	return NULL;
}

static inline fsnode* fsnodes_id_to_node(uint32_t id) {
	fsnode *p;
	uint32_t nodepos = NODEHASHPOS(id);
	for (p=gMetadata->nodehash[nodepos]; p ; p=p->next) {
		if (p->id == id) {
			return p;
		}
	}
	return NULL;
}

// returns true iff f is ancestor of p
static inline bool fsnodes_isancestor(fsnode *f,fsnode *p) {
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

// returns true iff p is reserved or in trash or f is ancestor of p
static inline bool fsnodes_isancestor_or_node_reserved_or_trash(fsnode *f,fsnode *p) {
	// Return 1 if file is reservered:
	if (p && (p->type == TYPE_RESERVED || p->type == TYPE_TRASH)) {
		return 1;
	}
	// Or if f is ancestor of p
	return fsnodes_isancestor(f, p);
}

// quota

static bool fsnodes_inode_quota_exceeded(uint32_t uid, uint32_t gid) {
	return gMetadata->gQuotaDatabase.isExceeded(QuotaRigor::kHard, QuotaResource::kInodes, uid, gid);
}

static bool fsnodes_size_quota_exceeded(uint32_t uid, uint32_t gid) {
	return gMetadata->gQuotaDatabase.isExceeded(QuotaRigor::kHard, QuotaResource::kSize, uid, gid);
}

static void fsnodes_quota_register_inode(fsnode *node) {
	gMetadata->gQuotaDatabase.changeUsage(QuotaResource::kInodes, node->uid, node->gid, +1);
}

static void fsnodes_quota_unregister_inode(fsnode *node) {
	gMetadata->gQuotaDatabase.changeUsage(QuotaResource::kInodes, node->uid, node->gid, -1);
}

static void fsnodes_quota_update_size(fsnode *node, int64_t delta) {
	if (delta != 0) {
		gMetadata->gQuotaDatabase.changeUsage(QuotaResource::kSize, node->uid, node->gid, delta);
	}
}

// stats
static inline void fsnodes_get_stats(fsnode *node,statsrecord *sr) {
	uint32_t i,lastchunk,lastchunksize;
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
		sr->chunks = 0;
		sr->length = node->data.fdata.length;
		sr->size = 0;
		if (node->data.fdata.length>0) {
			lastchunk = (node->data.fdata.length-1)>>MFSCHUNKBITS;
			lastchunksize = ((((node->data.fdata.length-1)&MFSCHUNKMASK)+MFSBLOCKSIZE)&MFSBLOCKNEGMASK)+MFSHDRSIZE;
		} else {
			lastchunk = 0;
			lastchunksize = MFSHDRSIZE;
		}
		for (i=0 ; i<node->data.fdata.chunks ; i++) {
			if (node->data.fdata.chunktab[i]>0) {
				if (i<lastchunk) {
					sr->size+=MFSCHUNKSIZE+MFSHDRSIZE;
				} else if (i==lastchunk) {
					sr->size+=lastchunksize;
				}
				sr->chunks++;
			}
		}
		sr->realsize = sr->size * node->goal;
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
		if (parent!=gMetadata->root) {
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
		if (parent!=gMetadata->root) {
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

static inline void fsnodes_fill_attr(fsnode *node,fsnode *parent,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t sesflags, Attributes& attr) {
#ifdef METARESTORE
	mabort("Bad code path - fsnodes_fill_attr() shall not be executed in metarestore context.");
#endif /* METARESTORE */
	uint8_t *ptr;
	uint16_t mode;
	uint32_t nlink;
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

static inline void fsnodes_fill_attr(const FsContext& context,
		fsnode *node, fsnode *parent, Attributes& attr) {
#ifdef METARESTORE
	mabort("Bad code path - fsnodes_fill_attr() shall not be executed in metarestore context.");
#endif /* METARESTORE */
	sassert(context.hasSessionData() && context.hasUidGidData());
	fsnodes_fill_attr(node, parent,
			context.uid(), context.gid(), context.auid(), context.agid(),
			context.sesflags(), attr);
}

static inline void fsnodes_remove_edge(uint32_t ts,fsedge *e) {
	statsrecord sr;
	removeFromChecksum(gMetadata->gFsEdgesChecksum, e->checksum);
	if (e->parent) {
		fsnodes_get_stats(e->child,&sr);
		fsnodes_sub_stats(e->parent,&sr);
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
	if (e->prev) {
		*(e->prev) = e->next;
		if (e->next) {
			e->next->prev = e->prev;
		}
	}
	delete e;
}

static inline void fsnodes_link(uint32_t ts,fsnode *parent,fsnode *child,uint16_t nleng,const uint8_t *name) {
	fsedge *e;
	statsrecord sr;
	uint32_t hpos;

	e = new fsedge;
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
	hpos = EDGEHASHPOS(fsnodes_hash(parent->id,nleng,name));
	e->next = gMetadata->edgehash[hpos];
	if (e->next) {
		e->next->prev = &(e->next);
	}
	gMetadata->edgehash[hpos] = e;
	e->prev = &(gMetadata->edgehash[hpos]);
	e->checksum = 0;
	fsedges_update_checksum(e);

	parent->data.ddata.elements++;
	if (child->type==TYPE_DIRECTORY) {
		parent->data.ddata.nlink++;
	}
	fsnodes_get_stats(child,&sr);
	fsnodes_add_stats(parent,&sr);
	if (ts>0) {
		parent->mtime = parent->ctime = ts;
		fsnodes_update_checksum(parent);
		child->ctime = ts;
		fsnodes_update_checksum(child);
	}
}

static inline fsnode* fsnodes_create_node(uint32_t ts, fsnode *node, uint16_t nleng,
		const uint8_t *name, uint8_t type, uint16_t mode, uint16_t umask, uint32_t uid,
		uint32_t gid, uint8_t copysgid, AclInheritance inheritacl) {
	fsnode *p;
	statsrecord *sr;
	uint32_t nodepos;
	p = new fsnode(type);
	gMetadata->nodes++;
	if (type==TYPE_DIRECTORY) {
		gMetadata->dirnodes++;
	}
	if (type==TYPE_FILE) {
		gMetadata->filenodes++;
	}
/* create node */
	p->id = fsnodes_get_next_id();
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
		sr = (statsrecord*) malloc(sizeof(statsrecord));
		passert(sr);
		memset(sr,0,sizeof(statsrecord));
		p->data.ddata.stats = sr;
		p->data.ddata.children = NULL;
		p->data.ddata.nlink = 2;
		p->data.ddata.elements = 0;
		break;
	case TYPE_FILE:
		p->data.fdata.length = 0;
		p->data.fdata.chunks = 0;
		p->data.fdata.chunktab = NULL;
		p->data.fdata.sessionids = NULL;
		break;
	case TYPE_SYMLINK:
		p->data.sdata.pleng = 0;
		p->data.sdata.path = NULL;
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		p->data.devdata.rdev = 0;
	}
	p->parents = NULL;
	nodepos = NODEHASHPOS(p->id);
	p->next = gMetadata->nodehash[nodepos];
	gMetadata->nodehash[nodepos] = p;
	p->checksum = 0;
	fsnodes_update_checksum(p);
	fsnodes_link(ts,node,p,nleng,name);
	fsnodes_quota_register_inode(p);
	if (type == TYPE_FILE) {
		fsnodes_quota_update_size(p, +fsnodes_get_size(p));
	}
	return p;
}

#ifndef METARESTORE
static inline uint32_t fsnodes_getpath_size(fsedge *e) {
	uint32_t size;
	fsnode *p;
	if (e==NULL) {
		return 0;
	}
	p = e->parent;
	size = e->nleng;
	while (p!=gMetadata->root && p->parents) {
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
	while (p!=gMetadata->root && p->parents) {
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
#endif /* METARESTORE */

static inline void fsnodes_getpath(fsedge *e,uint16_t *pleng,uint8_t **path) {
	uint32_t size;
	uint8_t *ret;
	fsnode *p;

	p = e->parent;
	size = e->nleng;
	while (p!=gMetadata->root && p->parents) {
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
	while (p!=gMetadata->root && p->parents) {
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
	Attributes attr;
	if (withattr) {
		fsnodes_fill_attr(p, p, uid, gid, auid, agid, sesflags, attr);
		::memcpy(dbuff, attr, sizeof (attr));
		dbuff += sizeof (attr);
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
			fsnodes_fill_attr(p, p, uid, gid, auid, agid, sesflags, attr);
			::memcpy(dbuff, attr, sizeof (attr));
			dbuff += sizeof (attr);
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
				fsnodes_fill_attr(p->parents->parent, p, uid, gid, auid, agid, sesflags, attr);
				::memcpy(dbuff, attr, sizeof (attr));
			} else {
				if (rootinode==MFS_ROOT_ID) {
					fsnodes_fill_attr(gMetadata->root, p, uid, gid, auid, agid, sesflags, attr);
					::memcpy(dbuff, attr, sizeof (attr));
				} else {
					fsnode *rn = fsnodes_id_to_node(rootinode);
					if (rn) {       // it should be always true because it's checked before, but better check than sorry
						fsnodes_fill_attr(rn, p, uid, gid, auid, agid, sesflags, attr);
						::memcpy(dbuff, attr, sizeof (attr));
					} else {
						memset(dbuff,0,sizeof (attr));
					}
				}
			}
			dbuff += sizeof (attr);
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
			fsnodes_fill_attr(e->child, p, uid, gid, auid, agid, sesflags, attr);
			::memcpy(dbuff, attr, sizeof (attr));
			dbuff += sizeof (attr);
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
	statsrecord psr,nsr;

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
	fsnodes_get_stats(dstobj,&psr);
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
		gMetadata->trashspace -= dstobj->data.fdata.length;
		gMetadata->trashspace += length;
	} else if (dstobj->type==TYPE_RESERVED) {
		gMetadata->reservedspace -= dstobj->data.fdata.length;
		gMetadata->reservedspace += length;
	}
	dstobj->data.fdata.length = length;
	fsnodes_get_stats(dstobj,&nsr);
	fsnodes_quota_update_size(dstobj, nsr.size - psr.size);
	for (fsedge *e=dstobj->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	dstobj->mtime = ts;
	dstobj->atime = ts;
	srcobj->atime = ts;
	fsnodes_update_checksum(srcobj);
	fsnodes_update_checksum(dstobj);
	return STATUS_OK;
}

static inline void fsnodes_changefilegoal(fsnode *obj,uint8_t goal) {
	uint32_t i;
	statsrecord psr,nsr;
	fsedge *e;

	fsnodes_get_stats(obj,&psr);
	nsr = psr;
	nsr.realsize = goal * nsr.size;
	for (e=obj->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	for (i=0 ; i<obj->data.fdata.chunks ; i++) {
		if (obj->data.fdata.chunktab[i]>0) {
			chunk_change_file(obj->data.fdata.chunktab[i],obj->goal,goal);
		}
	}
	obj->goal = goal;
	fsnodes_update_checksum(obj);
}

static inline void fsnodes_setlength(fsnode *obj,uint64_t length) {
	uint32_t i,chunks;
	uint64_t chunkid;
	statsrecord psr,nsr;
	fsnodes_get_stats(obj,&psr);
	if (obj->type==TYPE_TRASH) {
		gMetadata->trashspace -= obj->data.fdata.length;
		gMetadata->trashspace += length;
	} else if (obj->type==TYPE_RESERVED) {
		gMetadata->reservedspace -= obj->data.fdata.length;
		gMetadata->reservedspace += length;
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
	fsnodes_get_stats(obj,&nsr);
	fsnodes_quota_update_size(obj, nsr.size - psr.size);
	for (fsedge *e=obj->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	fsnodes_update_checksum(obj);
}

static inline void fsnodes_change_uid_gid(fsnode *p, uint32_t uid, uint32_t gid) {
	int64_t size = 0;
	fsnodes_quota_unregister_inode(p);
	if (p->type == TYPE_FILE || p->type == TYPE_TRASH || p->type == TYPE_RESERVED) {
		size = fsnodes_get_size(p);
		fsnodes_quota_update_size(p, -size);
	}
	p->uid = uid;
	p->gid = gid;
	fsnodes_quota_register_inode(p);
	if (p->type == TYPE_FILE || p->type == TYPE_TRASH || p->type == TYPE_RESERVED) {
		fsnodes_quota_update_size(p, +size);
	}
}

static inline void fsnodes_remove_node(uint32_t ts,fsnode *toremove) {
	uint32_t nodepos;
	fsnode **ptr;
	if (toremove->parents!=NULL) {
		return;
	}
// remove from idhash
	nodepos = NODEHASHPOS(toremove->id);
	ptr = &(gMetadata->nodehash[nodepos]);
	while (*ptr) {
		if (*ptr==toremove) {
			*ptr=toremove->next;
			break;
		}
		ptr = &((*ptr)->next);
	}
	removeFromChecksum(gMetadata->gFsNodesChecksum, toremove->checksum);
// and free
	gMetadata->nodes--;
	if (toremove->type==TYPE_DIRECTORY) {
		gMetadata->dirnodes--;
	}
	if (toremove->type==TYPE_FILE || toremove->type==TYPE_TRASH || toremove->type==TYPE_RESERVED) {
		uint32_t i;
		uint64_t chunkid;
		fsnodes_quota_update_size(toremove, -fsnodes_get_size(toremove));
		gMetadata->filenodes--;
		for (i=0 ; i<toremove->data.fdata.chunks ; i++) {
			chunkid = toremove->data.fdata.chunktab[i];
			if (chunkid>0) {
				if (chunk_delete_file(chunkid,toremove->goal)!=STATUS_OK) {
					syslog(LOG_ERR,"structure error - chunk %016" PRIX64 " not found (inode: %" PRIu32 " ; index: %" PRIu32 ")",chunkid,toremove->id,i);
				}
			}
		}
	}
	fsnodes_free_id(toremove->id,ts);
	xattr_removeinode(toremove->id);
	fsnodes_quota_unregister_inode(toremove);
#ifndef METARESTORE
	dcm_modify(toremove->id,0);
#endif
	delete toremove;
}


static inline void fsnodes_unlink(uint32_t ts,fsedge *e) {
	fsnode *child;
	uint16_t pleng=0;
	uint8_t *path=NULL;

	child = e->child;
	if (child->parents->nextparent==NULL) { // last link
		if (child->type==TYPE_FILE && (child->trashtime>0 || child->data.fdata.sessionids!=NULL)) { // go to trash or reserved ? - get path
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
				e = new fsedge;
				e->nleng = pleng;
				e->name = path;
				e->child = child;
				e->parent = NULL;
				e->nextchild = gMetadata->trash;
				e->nextparent = NULL;
				e->prevchild = &gMetadata->trash;
				e->prevparent = &(child->parents);
				if (e->nextchild) {
					e->nextchild->prevchild = &(e->nextchild);
				}
				e->next = NULL;
				e->prev = NULL;
				gMetadata->trash = e;
				child->parents = e;
				gMetadata->trashspace += child->data.fdata.length;
				gMetadata->trashnodes++;
				e->checksum = 0;
				fsedges_update_checksum(e);
			} else if (child->data.fdata.sessionids!=NULL) {
				child->type = TYPE_RESERVED;
				fsnodes_update_checksum(child);
				e = new fsedge;
				e->nleng = pleng;
				e->name = path;
				e->child = child;
				e->parent = NULL;
				e->nextchild = gMetadata->reserved;
				e->nextparent = NULL;
				e->prevchild = &gMetadata->reserved;
				e->prevparent = &(child->parents);
				if (e->nextchild) {
					e->nextchild->prevchild = &(e->nextchild);
				}
				e->next = NULL;
				e->prev = NULL;
				gMetadata->reserved = e;
				child->parents = e;
				gMetadata->reservedspace += child->data.fdata.length;
				gMetadata->reservednodes++;
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
		gMetadata->trashspace -= p->data.fdata.length;
		gMetadata->trashnodes--;
		if (p->data.fdata.sessionids!=NULL) {
			p->type = TYPE_RESERVED;
			fsnodes_update_checksum(p);
			gMetadata->reservedspace += p->data.fdata.length;
			gMetadata->reservednodes++;
			*(e->prevchild) = e->nextchild;
			if (e->nextchild) {
				e->nextchild->prevchild = e->prevchild;
			}
			e->nextchild = gMetadata->reserved;
			e->prevchild = &(gMetadata->reserved);
			if (e->nextchild) {
				e->nextchild->prevchild = &(e->nextchild);
			}
			gMetadata->reserved = e;
			return 0;
		} else {
			fsnodes_remove_edge(ts,e);
			fsnodes_remove_node(ts,p);
			return 1;
		}
	} else if (p->type==TYPE_RESERVED) {
		gMetadata->reservedspace -= p->data.fdata.length;
		gMetadata->reservednodes--;
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
	p = gMetadata->root;
	is_new = 0;
	for (;;) {
		partleng=0;
		while ((partleng < pleng) && (path[partleng] != '/')) {
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
			gMetadata->trashspace -= node->data.fdata.length;
			gMetadata->trashnodes--;
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

static inline void fsnodes_getgoal_recursive(fsnode *node,uint8_t gmode,uint32_t fgtab[10],uint32_t dgtab[10]) {
	fsedge *e;

	if (node->type==TYPE_FILE || node->type==TYPE_TRASH || node->type==TYPE_RESERVED) {
		if (node->goal>9) {
			syslog(LOG_WARNING,"inode %" PRIu32 ": goal>9 !!! - fixing",node->id);
			fsnodes_changefilegoal(node,9);
		} else if (node->goal<1) {
			syslog(LOG_WARNING,"inode %" PRIu32 ": goal<1 !!! - fixing",node->id);
			fsnodes_changefilegoal(node,1);
		}
		fgtab[node->goal]++;
	} else if (node->type==TYPE_DIRECTORY) {
		if (node->goal>9) {
			syslog(LOG_WARNING,"inode %" PRIu32 ": goal>9 !!! - fixing",node->id);
			node->goal=9;
		} else if (node->goal<1) {
			syslog(LOG_WARNING,"inode %" PRIu32 ": goal<1 !!! - fixing",node->id);
			node->goal=1;
		}
		dgtab[node->goal]++;
		if (gmode==GMODE_RECURSIVE) {
			for (e = node->data.ddata.children ; e ; e=e->nextchild) {
				fsnodes_getgoal_recursive(e->child,gmode,fgtab,dgtab);
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

static inline void fsnodes_setgoal_recursive(fsnode *node,uint32_t ts,uint32_t uid,uint8_t goal,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	fsedge *e;
	uint8_t set;

	if (node->type==TYPE_FILE || node->type==TYPE_DIRECTORY || node->type==TYPE_TRASH || node->type==TYPE_RESERVED) {
		if ((node->mode&(EATTR_NOOWNER<<12))==0 && uid!=0 && node->uid!=uid) {
			(*nsinodes)++;
		} else {
			set=0;
			switch (smode&SMODE_TMASK) {
			case SMODE_SET:
				if (node->goal!=goal) {
					set=1;
				}
				break;
			case SMODE_INCREASE:
				if (node->goal<goal) {
					set=1;
				}
				break;
			case SMODE_DECREASE:
				if (node->goal>goal) {
					set=1;
				}
				break;
			}
			if (set) {
				if (node->type!=TYPE_DIRECTORY) {
					fsnodes_changefilegoal(node,goal);
					(*sinodes)++;
				} else {
					node->goal=goal;
					(*sinodes)++;
				}
				node->ctime = ts;
				fsnodes_update_checksum(node);
			} else {
				(*ncinodes)++;
			}
		}
		if (node->type==TYPE_DIRECTORY && (smode&SMODE_RMASK)) {
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
				statsrecord psr,nsr;
				fsnodes_unlink(ts,e);
				dstnode = fsnodes_create_node(ts,parentnode,nleng,name,TYPE_FILE,srcnode->mode,0,srcnode->uid,srcnode->gid,0,AclInheritance::kDontInheritAcl);
				fsnodes_get_stats(dstnode,&psr);
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
				fsnodes_get_stats(dstnode,&nsr);
				fsnodes_add_sub_stats(parentnode,&nsr,&psr);
				fsnodes_quota_update_size(dstnode, nsr.size - psr.size);
			}
		} else if (srcnode->type==TYPE_SYMLINK) {
			if (dstnode->data.sdata.pleng!=srcnode->data.sdata.pleng) {
				statsrecord sr;
				memset(&sr,0,sizeof(statsrecord));
				sr.length = dstnode->data.sdata.pleng-srcnode->data.sdata.pleng;
				fsnodes_add_stats(parentnode,&sr);
			}
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
	} else {
		if (srcnode->type==TYPE_FILE || srcnode->type==TYPE_DIRECTORY || srcnode->type==TYPE_SYMLINK || srcnode->type==TYPE_BLOCKDEV || srcnode->type==TYPE_CHARDEV || srcnode->type==TYPE_SOCKET || srcnode->type==TYPE_FIFO) {
			statsrecord psr,nsr;
			dstnode = fsnodes_create_node(ts,parentnode,nleng,name,srcnode->type,srcnode->mode,0,srcnode->uid,srcnode->gid,0,AclInheritance::kDontInheritAcl);
			fsnodes_get_stats(dstnode,&psr);
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
				fsnodes_get_stats(dstnode,&nsr);
				fsnodes_add_sub_stats(parentnode,&nsr,&psr);
				fsnodes_quota_update_size(dstnode, nsr.size - psr.size);
			} else if (srcnode->type==TYPE_SYMLINK) {
				if (srcnode->data.sdata.pleng>0) {
					dstnode->data.sdata.path = (uint8_t*) malloc(srcnode->data.sdata.pleng);
					passert(dstnode->data.sdata.path);
					memcpy(dstnode->data.sdata.path,srcnode->data.sdata.path,srcnode->data.sdata.pleng);
					dstnode->data.sdata.pleng = srcnode->data.sdata.pleng;
				}
				fsnodes_get_stats(dstnode,&nsr);
				fsnodes_add_sub_stats(parentnode,&nsr,&psr);
			} else if (srcnode->type==TYPE_BLOCKDEV || srcnode->type==TYPE_CHARDEV) {
				dstnode->data.devdata.rdev = srcnode->data.devdata.rdev;
			}
		}
	}
	fsnodes_update_checksum(dstnode);
}

static inline uint8_t fsnodes_snapshot_test(fsnode *origsrcnode,fsnode *srcnode,fsnode *parentnode,uint32_t nleng,const uint8_t *name,uint8_t canoverwrite) {
	fsedge *e;
	fsnode *dstnode;
	uint8_t status;
	if (fsnodes_inode_quota_exceeded(srcnode->uid, srcnode->gid)) {
		return ERROR_QUOTA;
	}
	if (srcnode->type == TYPE_FILE && fsnodes_size_quota_exceeded(srcnode->uid, srcnode->gid)) {
		return ERROR_QUOTA;
	}
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

// Arguments for verify_session
namespace {
	enum class SessionType { kNotMeta, kOnlyMeta, kAny };
	enum class OperationMode { kReadWrite, kReadOnly };
	enum class ExpectedNodeType { kFile, kDirectory, kNotDirectory, kAny };
}

static uint8_t verify_session(const FsContext& context,
		OperationMode operationMode, SessionType sessionType) {
	if (context.hasSessionData()
			&& (context.sesflags() & SESFLAG_READONLY)
			&& (operationMode == OperationMode::kReadWrite)) {
		return ERROR_EROFS;
	}
	if (context.hasSessionData()
			&& (context.rootinode() == 0)
			&& (sessionType == SessionType::kNotMeta)) {
		return ERROR_ENOENT;
	}
	if (context.hasSessionData()
			&& (context.rootinode() != 0)
			&& (sessionType == SessionType::kOnlyMeta)) {
		return ERROR_EPERM;
	}
	return STATUS_OK;
}

/*
 * Treating rootinode as the root of the hierarchy, converts (rootinode, inode) to fsnode*
 * ie:
 * * if inode == rootinode, then returns root node
 * * if inode != rootinode, then returns some node
 * Checks for permissions needed to perform the operation (defined by modemask)
 * Can return a reserved node or a node from trash
 */
static uint8_t fsnodes_get_node_for_operation(
		const FsContext& context, ExpectedNodeType expectedNodeType,
		uint8_t modemask, uint32_t inode, fsnode **ret) {
	fsnode *p;
	if (!context.hasSessionData()) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
	} else if (context.rootinode() == MFS_ROOT_ID || (context.rootinode() == 0)) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
		if (context.rootinode() == 0 && p->type != TYPE_TRASH && p->type != TYPE_RESERVED) {
			return ERROR_EPERM;
		}
	} else {
		fsnode *rn = fsnodes_id_to_node(context.rootinode());
		if (!rn || rn->type != TYPE_DIRECTORY) {
			return ERROR_ENOENT;
		}
		if (inode == MFS_ROOT_ID) {
			p = rn;
		} else {
			p = fsnodes_id_to_node(inode);
			if (!p) {
				return ERROR_ENOENT;
			}
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn, p)) {
				return ERROR_EPERM;
			}
		}
	}
	if ((expectedNodeType == ExpectedNodeType::kDirectory) && (p->type != TYPE_DIRECTORY)) {
		return ERROR_ENOTDIR;
	}
	if ((expectedNodeType == ExpectedNodeType::kNotDirectory) && (p->type == TYPE_DIRECTORY)) {
		return ERROR_EPERM;
	}
	if ((expectedNodeType == ExpectedNodeType::kFile) && (p->type != TYPE_FILE) && (p->type != TYPE_RESERVED) && (p->type != TYPE_TRASH)) {
		return ERROR_EPERM;
	}
	if (context.canCheckPermissions() &&
			!fsnodes_access(p, context.uid(), context.gid(), modemask, context.sesflags())) {
		return ERROR_EACCES;
	}
	*ret = p;
	return STATUS_OK;
}

/* master <-> fuse operations */

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

#ifndef METARESTORE
uint8_t fs_readreserved_size(uint32_t rootinode,uint8_t sesflags,uint32_t *dbuffsize) {
	if (rootinode!=0) {
		return ERROR_EPERM;
	}
	(void)sesflags;
	*dbuffsize = fsnodes_getdetachedsize(gMetadata->reserved);
	return STATUS_OK;
}

void fs_readreserved_data(uint32_t rootinode,uint8_t sesflags,uint8_t *dbuff) {
	(void)rootinode;
	(void)sesflags;
	fsnodes_getdetacheddata(gMetadata->reserved,dbuff);
}


uint8_t fs_readtrash_size(uint32_t rootinode,uint8_t sesflags,uint32_t *dbuffsize) {
	if (rootinode!=0) {
		return ERROR_EPERM;
	}
	(void)sesflags;
	*dbuffsize = fsnodes_getdetachedsize(gMetadata->trash);
	return STATUS_OK;
}

void fs_readtrash_data(uint32_t rootinode,uint8_t sesflags,uint8_t *dbuff) {
	(void)rootinode;
	(void)sesflags;
	fsnodes_getdetacheddata(gMetadata->trash,dbuff);
}

/* common procedure for trash and reserved files */
uint8_t fs_getdetachedattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,Attributes& attr,uint8_t dtype) {
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

uint8_t fs_settrashpath(const FsContext& context,
		uint32_t inode, uint32_t pleng, const uint8_t *path) {
	ChecksumUpdater cu(context.ts());
	fsnode *p;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kOnlyMeta);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kAny, MODE_MASK_EMPTY, inode, &p);
	if (status != STATUS_OK) {
		return status;
	} else if (p->type != TYPE_TRASH) {
		return ERROR_ENOENT;
	} else if (pleng == 0) {
		return ERROR_EINVAL;
	}
	for (uint32_t i = 0; i < pleng; i++) {
		if (path[i] == 0) {
			return ERROR_EINVAL;
		}
	}

	uint8_t *newpath = (uint8_t*) malloc(pleng);
	passert(newpath);
	free(p->parents->name);
	memcpy(newpath, path, pleng);
	p->parents->name = newpath;
	p->parents->nleng = pleng;
	fsedges_update_checksum(p->parents);
	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(),
				"SETPATH(%" PRIu32 ",%s)",
				p->id, fsnodes_escape_name(pleng, newpath));
	} else {
		gMetadata->metaversion++;
	}
	return STATUS_OK;
}

uint8_t fs_undel(const FsContext& context, uint32_t inode) {
	ChecksumUpdater cu(context.ts());
	fsnode *p;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kOnlyMeta);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kAny, MODE_MASK_EMPTY, inode, &p);
	if (status != STATUS_OK) {
		return status;
	} else if (p->type != TYPE_TRASH) {
		return ERROR_ENOENT;
	}

	status = fsnodes_undel(context.ts(), p);
	if (context.isPersonalityMaster()) {
		if (status == STATUS_OK) {
			fs_changelog(context.ts(), "UNDEL(%" PRIu32 ")", p->id);
		}
	} else {
		gMetadata->metaversion++;
	}
	return status;
}

uint8_t fs_purge(const FsContext& context, uint32_t inode) {
	ChecksumUpdater cu(context.ts());
	fsnode *p;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kOnlyMeta);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kAny, MODE_MASK_EMPTY, inode, &p);
	if (status != STATUS_OK) {
		return status;
	} else if (p->type != TYPE_TRASH) {
		return ERROR_ENOENT;
	}
	uint32_t purged_inode = p->id; // This should be equal to inode, because p is not a directory
	fsnodes_purge(context.ts(), p);

	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(), "PURGE(%" PRIu32 ")", purged_inode);
	} else {
		gMetadata->metaversion++;
	}
	return STATUS_OK;
}

#ifndef METARESTORE
void fs_info(uint64_t *totalspace,uint64_t *availspace,uint64_t *trspace,uint32_t *trnodes,uint64_t *respace,uint32_t *renodes,uint32_t *inodes,uint32_t *dnodes,uint32_t *fnodes) {
	matocsserv_getspace(totalspace,availspace);
	*trspace = gMetadata->trashspace;
	*trnodes = gMetadata->trashnodes;
	*respace = gMetadata->reservedspace;
	*renodes = gMetadata->reservednodes;
	*inodes = gMetadata->nodes;
	*dnodes = gMetadata->dirnodes;
	*fnodes = gMetadata->filenodes;
}

uint8_t fs_getrootinode(uint32_t *rootinode,const uint8_t *path) {
	uint32_t nleng;
	const uint8_t *name;
	fsnode *p;
	fsedge *e;

	name = path;
	p = gMetadata->root;
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
		*trspace = gMetadata->trashspace;
		*respace = gMetadata->reservedspace;
		rn = gMetadata->root;
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
#endif /* #ifndef METARESTORE */


uint8_t fs_apply_checksum(const std::string& version, uint64_t checksum) {
	std::string versionString = lizardfsVersionToString(LIZARDFS_VERSHEX);
	uint64_t computedChecksum = fs_checksum(ChecksumMode::kGetCurrent);
	gMetadata->metaversion++;
	if (version == versionString) {
		if (checksum != computedChecksum) {
			return ERROR_MISMATCH;
		}
	}
	return STATUS_OK;
}

uint8_t fs_apply_access(uint32_t ts,uint32_t inode) {
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	p->atime = ts;
	fsnodes_update_checksum(p);
	gMetadata->metaversion++;
	return STATUS_OK;
}

#ifndef METARESTORE
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	return fsnodes_access(p,uid,gid,modemask,sesflags)?STATUS_OK:ERROR_EACCES;
}

uint8_t fs_lookup(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint32_t *inode,Attributes& attr) {
	fsnode *wd,*rn;
	fsedge *e;

	*inode = 0;
	memset(attr,0,35);
	if (rootinode==MFS_ROOT_ID) {
		rn = gMetadata->root;
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,wd)) {
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
	*inode = e->child->id;
	fsnodes_fill_attr(e->child,wd,uid,gid,auid,agid,sesflags,attr);
	stats_lookup++;
	return STATUS_OK;
}

uint8_t fs_getattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,Attributes& attr) {
	fsnode *p,*rn;

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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	fsnodes_fill_attr(p,NULL,uid,gid,auid,agid,sesflags,attr);
	stats_getattr++;
	return STATUS_OK;
}

uint8_t fs_try_setlength(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint64_t length,Attributes& attr,uint64_t *chunkid) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
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
				status = chunk_multi_truncate(ochunkid, (length & MFSCHUNKMASK),
						p->goal, fsnodes_size_quota_exceeded(p->uid, p->gid), &nchunkid);
				if (status!=STATUS_OK) {
					return status;
				}
				p->data.fdata.chunktab[indx] = nchunkid;
				*chunkid = nchunkid;
				fs_changelog(ts,
						"TRUNC(%" PRIu32 ",%" PRIu32 "):%" PRIu64,
						inode, indx, nchunkid);
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

uint8_t fs_apply_trunc(uint32_t ts,uint32_t inode,uint32_t indx,uint64_t chunkid) {
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
	if (ochunkid == 0) {
		return ERROR_NOCHUNK;
	}
	status = chunk_apply_modification(ts, ochunkid, p->goal, true, &nchunkid);
	if (status!=STATUS_OK) {
		return status;
	}
	if (chunkid!=nchunkid) {
		return ERROR_MISMATCH;
	}
	p->data.fdata.chunktab[indx] = nchunkid;
	gMetadata->metaversion++;
	fsnodes_update_checksum(p);
	return STATUS_OK;
}

uint8_t fs_set_nextchunkid(const FsContext& context, uint64_t nextChunkId) {
	ChecksumUpdater cu(context.ts());
	uint8_t status = chunk_set_next_chunkid(nextChunkId);
	if (context.isPersonalityMaster()) {
		if (status == STATUS_OK) {
			fs_changelog(context.ts(), "NEXTCHUNKID(%" PRIu64 ")", nextChunkId);
		}
	} else {
		gMetadata->metaversion++;
	}
	return status;
}

#ifndef METARESTORE
uint8_t fs_end_setlength(uint64_t chunkid) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fs_changelog(ts, "UNLOCK(%" PRIu64 ")", chunkid);
	return chunk_unlock(chunkid);
}
#endif

uint8_t fs_apply_unlock(uint64_t chunkid) {
	gMetadata->metaversion++;
	return chunk_unlock(chunkid);
}

#ifndef METARESTORE
uint8_t fs_do_setlength(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint64_t length,Attributes& attr) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *p = NULL;

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
		fsnode* rn = fsnodes_id_to_node(rootinode);
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	fsnodes_setlength(p,length);
	fs_changelog(ts, "LENGTH(%" PRIu32 ",%" PRIu64 ")",inode,p->data.fdata.length);
	p->ctime = p->mtime = ts;
	fsnodes_update_checksum(p);
	fsnodes_fill_attr(p,NULL,uid,gid,auid,agid,sesflags,attr);
	stats_setattr++;
	return STATUS_OK;
}


uint8_t fs_setattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t setmask,uint16_t attrmode,uint32_t attruid,uint32_t attrgid,uint32_t attratime,uint32_t attrmtime,SugidClearMode sugidclearmode,Attributes& attr) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *p = NULL;

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
		fsnode* rn = fsnodes_id_to_node(rootinode);
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
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
		case SugidClearMode::kAlways:
			p->mode &= 0171777; // safest approach - always delete both suid and sgid
			attrmode &= 01777;
			break;
		case SugidClearMode::kOsx:
			if (uid!=0) { // OSX+Solaris - every change done by unprivileged user should clear suid and sgid
				p->mode &= 0171777;
				attrmode &= 01777;
			}
			break;
		case SugidClearMode::kBsd:
			if (uid!=0 && (setmask&SET_GID_FLAG) && p->gid!=attrgid) { // *BSD - like in kOsx but only when something is actually changed
				p->mode &= 0171777;
				attrmode &= 01777;
			}
			break;
		case SugidClearMode::kExt:
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
		case SugidClearMode::kXfs:
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
		case SugidClearMode::kNever:
			break;
		}
	}
	if (setmask&SET_MODE_FLAG) {
		p->mode = (attrmode & 07777) | (p->mode & 0xF000);
	}
	if (setmask & (SET_UID_FLAG | SET_GID_FLAG)) {
		fsnodes_change_uid_gid(p,
				((setmask & SET_UID_FLAG) ? attruid : p->uid),
				((setmask & SET_GID_FLAG) ? attrgid : p->gid));
	}
	if (setmask & SET_ATIME_NOW_FLAG) {
		p->atime = ts;
	} else if (setmask & SET_ATIME_FLAG) {
		p->atime = attratime;
	}
	if (setmask & SET_MTIME_NOW_FLAG) {
		p->mtime = ts;
	} else if (setmask & SET_MTIME_FLAG) {
		p->mtime = attrmtime;
	}
	fs_changelog(ts, "ATTR(%" PRIu32 ",%" PRIu16",%" PRIu32 ",%" PRIu32 ",%" PRIu32 ",%" PRIu32 ")",inode,p->mode & 07777,p->uid,p->gid,p->atime,p->mtime);
	p->ctime = ts;
	fsnodes_fill_attr(p,NULL,uid,gid,auid,agid,sesflags,attr);
	fsnodes_update_checksum(p);
	stats_setattr++;
	return STATUS_OK;
}
#endif


uint8_t fs_apply_attr(uint32_t ts,uint32_t inode,uint32_t mode,uint32_t uid,uint32_t gid,uint32_t atime,uint32_t mtime) {
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (mode>07777) {
		return ERROR_EINVAL;
	}
	p->mode = mode | (p->mode & 0xF000);
	if (p->uid != uid || p->gid != gid) {
		fsnodes_change_uid_gid(p, uid, gid);
	}
	p->atime = atime;
	p->mtime = mtime;
	p->ctime = ts;
	fsnodes_update_checksum(p);
	gMetadata->metaversion++;
	return STATUS_OK;
}

uint8_t fs_apply_length(uint32_t ts,uint32_t inode,uint64_t length) {
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
	gMetadata->metaversion++;
	return STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_readlink(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t *pleng,uint8_t **path) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *p = NULL;

	(void)sesflags;
	*pleng = 0;
	*path = NULL;
	if (rootinode==MFS_ROOT_ID) {
		p = fsnodes_id_to_node(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
	} else {
		fsnode* rn = fsnodes_id_to_node(rootinode);
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
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
		fs_changelog(ts, "ACCESS(%" PRIu32 ")",inode);
	}
	stats_readlink++;
	return STATUS_OK;
}
#endif

uint8_t fs_symlink(
		const FsContext& context,
		uint32_t parent,
		uint16_t nleng, const uint8_t *name,
		uint32_t pleng, const uint8_t *path,
		uint32_t *inode, Attributes* attr) {
	ChecksumUpdater cu(context.ts());
	fsnode *wd;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kDirectory, MODE_MASK_W, parent, &wd);
	if (status != STATUS_OK) {
		return status;
	}
	if (pleng == 0) {
		return ERROR_EINVAL;
	}
	for (uint32_t i = 0; i < pleng; i++) {
		if (path[i] == 0) {
			return ERROR_EINVAL;
		}
	}
	if (fsnodes_namecheck(nleng, name) < 0) {
		return ERROR_EINVAL;
	}
	if (fsnodes_nameisused(wd, nleng, name)) {
		return ERROR_EEXIST;
	}
	if ((context.isPersonalityMaster())
			&& fsnodes_inode_quota_exceeded(context.uid(), context.gid())) {
		return ERROR_QUOTA;
	}
	uint8_t *newpath = (uint8_t*) malloc(pleng);
	passert(newpath);
	fsnode *p = fsnodes_create_node(
			context.ts(), wd, nleng, name, TYPE_SYMLINK, 0777, 0, context.uid(), context.gid(), 0,
			AclInheritance::kDontInheritAcl);
	memcpy(newpath,path,pleng);
	p->data.sdata.path = newpath;
	p->data.sdata.pleng = pleng;
	fsnodes_update_checksum(p);
	statsrecord sr;
	memset(&sr,0,sizeof(statsrecord));
	sr.length = pleng;
	fsnodes_add_stats(wd,&sr);
	if (attr != NULL) {
		fsnodes_fill_attr(context, p, wd, *attr);
	}
	if (context.isPersonalityMaster()) {
		*inode = p->id;
		fs_changelog(context.ts(),
				"SYMLINK(%" PRIu32 ",%s,%s,%" PRIu32 ",%" PRIu32 "):%" PRIu32,
				wd->id,
				fsnodes_escape_name(nleng, name),
				fsnodes_escape_name(pleng, newpath),
				context.uid(), context.gid(), p->id);
	} else {
		if (*inode != p->id) {
			return ERROR_MISMATCH;
		}
		gMetadata->metaversion++;
	}
#ifndef METARESTORE
	stats_symlink++;
#endif /* #ifndef METARESTORE */
	return STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_mknod(uint32_t rootinode, uint8_t sesflags, uint32_t parent, uint16_t nleng,
		const uint8_t *name, uint8_t type, uint16_t mode, uint16_t umask, uint32_t uid,
		uint32_t gid, uint32_t auid, uint32_t agid, uint32_t rdev,
		uint32_t *inode, Attributes& attr) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,wd)) {
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
	p = fsnodes_create_node(ts, wd, nleng, name, type, mode,
			umask, uid, gid, 0, AclInheritance::kInheritAcl);
	if (type==TYPE_BLOCKDEV || type==TYPE_CHARDEV) {
		p->data.devdata.rdev = rdev;
	}
	*inode = p->id;
	fsnodes_fill_attr(p,wd,uid,gid,auid,agid,sesflags,attr);
	fs_changelog(ts,
			"CREATE(%" PRIu32 ",%s,%c,%" PRIu16",%" PRIu32 ",%" PRIu32 ",%" PRIu32 "):%" PRIu32,
			parent, fsnodes_escape_name(nleng, name), type, p->mode & 07777, uid, gid, rdev, p->id);
	stats_mknod++;
	fsnodes_update_checksum(p);
	return STATUS_OK;
}

uint8_t fs_mkdir(uint32_t rootinode, uint8_t sesflags, uint32_t parent, uint16_t nleng,
		const uint8_t *name, uint16_t mode, uint16_t umask, uint32_t uid, uint32_t gid,
		uint32_t auid, uint32_t agid, uint8_t copysgid, uint32_t *inode, Attributes& attr) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,wd)) {
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
	p = fsnodes_create_node(ts, wd, nleng, name, TYPE_DIRECTORY, mode,
			umask, uid, gid, copysgid, AclInheritance::kInheritAcl);
	*inode = p->id;
	fsnodes_fill_attr(p,wd,uid,gid,auid,agid,sesflags,attr);
	fs_changelog(ts,
			"CREATE(%" PRIu32 ",%s,%c,%" PRIu16",%" PRIu32 ",%" PRIu32 ",%" PRIu32 "):%" PRIu32,
			parent, fsnodes_escape_name(nleng, name), TYPE_DIRECTORY, p->mode & 07777,
			uid, gid, 0, p->id);
	stats_mkdir++;
	return STATUS_OK;
}
#endif

uint8_t fs_apply_create(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,uint8_t type,uint32_t mode,uint32_t uid,uint32_t gid,uint32_t rdev,uint32_t inode) {
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
	p = fsnodes_create_node(ts,wd,nleng,name,type,mode,0,uid,gid,0,AclInheritance::kInheritAcl);
	if (type==TYPE_BLOCKDEV || type==TYPE_CHARDEV) {
		p->data.devdata.rdev = rdev;
		fsnodes_update_checksum(p);
	}
	if (inode!=p->id) {
		return ERROR_MISMATCH;
	}
	gMetadata->metaversion++;
	return STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_unlink(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *wd,*rn;
	fsedge *e;
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,wd)) {
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
	fs_changelog(ts, "UNLINK(%" PRIu32 ",%s):%" PRIu32,parent,fsnodes_escape_name(nleng,name),e->child->id);
	fsnodes_unlink(ts,e);
	stats_unlink++;
	return STATUS_OK;
}

uint8_t fs_rmdir(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *wd,*rn;
	fsedge *e;
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,wd)) {
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
	fs_changelog(ts, "UNLINK(%" PRIu32 ",%s):%" PRIu32,parent,fsnodes_escape_name(nleng,name),e->child->id);
	fsnodes_unlink(ts,e);
	stats_rmdir++;
	return STATUS_OK;
}
#endif

uint8_t fs_apply_unlink(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,uint32_t inode) {
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
	gMetadata->metaversion++;
	return STATUS_OK;
}

uint8_t fs_rename(const FsContext& context,
		uint32_t parent_src, uint16_t nleng_src, const uint8_t *name_src,
		uint32_t parent_dst, uint16_t nleng_dst, const uint8_t *name_dst,
		uint32_t *inode, Attributes* attr) {
	ChecksumUpdater cu(context.ts());
	fsnode *swd;
	fsnode *dwd;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kDirectory, MODE_MASK_W, parent_dst, &dwd);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kDirectory, MODE_MASK_W, parent_src, &swd);
	if (status != STATUS_OK) {
		return status;
	}
	if (fsnodes_namecheck(nleng_src, name_src) < 0) {
		return ERROR_EINVAL;
	}
	fsedge *se = fsnodes_lookup(swd, nleng_src, name_src);
	if (!se) {
		return ERROR_ENOENT;
	}
	fsnode *node = se->child;
	if (context.canCheckPermissions() && !fsnodes_sticky_access(swd, node, context.uid())) {
		return ERROR_EPERM;
	}
	if ((context.personality() != metadataserver::Personality::kMaster) && (node->id != *inode)) {
		return ERROR_MISMATCH;
	} else {
		*inode = node->id;
	}
	if (se->child->type == TYPE_DIRECTORY) {
		if (fsnodes_isancestor(se->child, dwd)) {
			return ERROR_EINVAL;
		}
	}
	if (fsnodes_namecheck(nleng_dst, name_dst) < 0) {
		return ERROR_EINVAL;
	}
	fsedge *de = fsnodes_lookup(dwd,nleng_dst,name_dst);
	if (de) {
		if (de->child->type == TYPE_DIRECTORY && de->child->data.ddata.children != NULL) {
			return ERROR_ENOTEMPTY;
		}
		if (context.canCheckPermissions() && !fsnodes_sticky_access(dwd, de->child, context.uid())) {
			return ERROR_EPERM;
		}
		fsnodes_unlink(context.ts(), de);
	}
	fsnodes_remove_edge(context.ts(), se);
	fsnodes_link(context.ts(), dwd, node, nleng_dst, name_dst);
	if (attr) {
		fsnodes_fill_attr(context, node, dwd, *attr);
	}
	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(),
				"MOVE(%" PRIu32 ",%s,%" PRIu32 ",%s):%" PRIu32,
				swd->id, fsnodes_escape_name(nleng_src, name_src),
				dwd->id, fsnodes_escape_name(nleng_dst, name_dst),
				node->id);
	} else {
		gMetadata->metaversion++;
	}
#ifndef METARESTORE
	stats_rename++;
#endif
	return STATUS_OK;
}

uint8_t fs_link(const FsContext& context,
		uint32_t inode_src, uint32_t parent_dst, uint16_t nleng_dst, const uint8_t *name_dst,
		uint32_t *inode, Attributes* attr) {
	ChecksumUpdater cu(context.ts());
	fsnode *sp;
	fsnode *dwd;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kDirectory, MODE_MASK_W, parent_dst, &dwd);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kNotDirectory, MODE_MASK_EMPTY, inode_src, &sp);
	if (status != STATUS_OK) {
		return status;
	}
	if (sp->type == TYPE_TRASH || sp->type == TYPE_RESERVED) {
		return ERROR_ENOENT;
	}
	if (fsnodes_namecheck(nleng_dst, name_dst) < 0) {
		return ERROR_EINVAL;
	}
	if (fsnodes_nameisused(dwd, nleng_dst, name_dst)) {
		return ERROR_EEXIST;
	}
	fsnodes_link(context.ts(), dwd, sp, nleng_dst, name_dst);
	if (inode) {
		*inode = inode_src;
	}
	if (attr) {
		fsnodes_fill_attr(context, sp, dwd, *attr);
	}
	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(),
				"LINK(%" PRIu32 ",%" PRIu32 ",%s)",
				sp->id, dwd->id, fsnodes_escape_name(nleng_dst, name_dst));
	} else {
		gMetadata->metaversion++;
	}
#ifndef METARESTORE
	stats_link++;
#endif
	return STATUS_OK;
}

uint8_t fs_snapshot(const FsContext& context,
		uint32_t inode_src, uint32_t parent_dst, uint16_t nleng_dst, const uint8_t *name_dst,
		uint8_t canoverwrite) {
	ChecksumUpdater cu(context.ts());
	fsnode *sp = NULL;
	fsnode *dwd = NULL;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kDirectory, MODE_MASK_W, parent_dst, &dwd);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kAny, MODE_MASK_R, inode_src, &sp);
	if (status != STATUS_OK) {
		return status;
	}
	if (sp->type == TYPE_DIRECTORY) {
		if (sp == dwd || fsnodes_isancestor(sp, dwd)) {
			return ERROR_EINVAL;
		}
	}
	status = fsnodes_snapshot_test(sp, sp, dwd, nleng_dst, name_dst, canoverwrite);
	if (status != STATUS_OK) {
		return status;
	}
	fsnodes_snapshot(context.ts(), sp, dwd, nleng_dst, name_dst);
	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(),
				"SNAPSHOT(%" PRIu32 ",%" PRIu32 ",%s,%" PRIu8 ")",
				sp->id, dwd->id, fsnodes_escape_name(nleng_dst, name_dst), canoverwrite);
	} else {
		gMetadata->metaversion++;
	}
	return STATUS_OK;
}

uint8_t fs_append(const FsContext& context, uint32_t inode, uint32_t inode_src) {
	ChecksumUpdater cu(context.ts());
	fsnode *p, *sp;
	if (inode == inode_src) {
		return ERROR_EINVAL;
	}
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kFile, MODE_MASK_W, inode, &p);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kFile, MODE_MASK_R, inode_src, &sp);
	if (status != STATUS_OK) {
		return status;
	}
	if (context.isPersonalityMaster()
			&& fsnodes_size_quota_exceeded(p->uid, p->gid)) {
		return ERROR_QUOTA;
	}
	status = fsnodes_appendchunks(context.ts(), p, sp);
	if (status != STATUS_OK) {
		return status;
	}
	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(), "APPEND(%" PRIu32 ",%" PRIu32 ")", p->id, sp->id);
	} else {
		gMetadata->metaversion++;
	}
	return status;
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
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
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *p = (fsnode*)dnode;
	if (p->atime!=ts) {
		p->atime = ts;
		fsnodes_update_checksum(p);
		fs_changelog(ts, "ACCESS(%" PRIu32 ")",p->id);
		fsnodes_getdirdata(rootinode,uid,gid,auid,agid,sesflags,p,dbuff,flags&GETDIR_FLAG_WITHATTR);
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
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

uint8_t fs_opencheck(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t flags,Attributes& attr) {
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
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

uint8_t fs_acquire(const FsContext& context, uint32_t inode, uint32_t sessionid) {
	ChecksumUpdater cu(context.ts());
#ifndef METARESTORE
	if (context.isPersonalityShadow()) {
		matoclserv_add_open_file(sessionid, inode);
	}
#endif /* #ifndef METARESTORE */
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
	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(), "ACQUIRE(%" PRIu32 ",%" PRIu32 ")", inode, sessionid);
	} else {
		gMetadata->metaversion++;
	}
	return STATUS_OK;
}

uint8_t fs_release(const FsContext& context, uint32_t inode, uint32_t sessionid) {
	ChecksumUpdater cu(context.ts());
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
			if (context.isPersonalityShadow()) {
				matoclserv_remove_open_file(sessionid, inode);
			}
#endif /* #ifndef METARESTORE */
			if (context.isPersonalityMaster()) {
				fs_changelog(context.ts(), "RELEASE(%" PRIu32 ",%" PRIu32 ")", inode, sessionid);
			} else {
				gMetadata->metaversion++;
			}
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
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fs_changelog(ts, "SESSION():%" PRIu32, gMetadata->nextsessionid);
	return gMetadata->nextsessionid++;
}
#endif
uint8_t fs_apply_session(uint32_t sessionid) {
	if (sessionid!=gMetadata->nextsessionid) {
		return ERROR_MISMATCH;
	}
	gMetadata->metaversion++;
	gMetadata->nextsessionid++;
	return STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint64_t *chunkid,uint64_t *length) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *p;

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
		fs_changelog(ts, "ACCESS(%" PRIu32 ")",inode);
	}
	stats_read++;
	return STATUS_OK;
}
#endif

uint8_t fs_writechunk(const FsContext& context, uint32_t inode, uint32_t indx,
		uint64_t *chunkid, uint8_t *opflag, uint64_t *length) {
	ChecksumUpdater cu(context.ts());
	uint32_t i;
	uint64_t ochunkid, nchunkid;
	fsnode *p;

	uint8_t status = verify_session(context,
			OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kFile, MODE_MASK_EMPTY, inode, &p);
	if (status != STATUS_OK) {
		return status;
	}
	if (indx > MAX_INDEX) {
		return ERROR_INDEXTOOBIG;
	}

	const bool quota_exceeded = fsnodes_size_quota_exceeded(p->uid, p->gid);
	statsrecord psr;
	fsnodes_get_stats(p, &psr);

	/* resize chunks structure */
	if (indx >= p->data.fdata.chunks) {
		if (context.isPersonalityMaster() && quota_exceeded) {
			return ERROR_QUOTA;
		}
		uint32_t newsize;
		if (indx < 8) {
			newsize = indx + 1;
		} else if (indx < 64) {
			newsize=(indx & 0xFFFFFFF8) + 8;
		} else {
			newsize = (indx & 0xFFFFFFC0) + 64;
		}
		if (p->data.fdata.chunktab == NULL) {
			p->data.fdata.chunktab = (uint64_t*)malloc(sizeof(uint64_t) * newsize);
		} else {
			p->data.fdata.chunktab = (uint64_t*)realloc(p->data.fdata.chunktab,
					sizeof(uint64_t) * newsize);
		}
		passert(p->data.fdata.chunktab);
		for (i = p->data.fdata.chunks; i < newsize; i++) {
			p->data.fdata.chunktab[i] = 0;
		}
		p->data.fdata.chunks = newsize;
	}

	ochunkid = p->data.fdata.chunktab[indx];
	if (context.isPersonalityMaster()) {
#ifndef METARESTORE
		status = chunk_multi_modify(ochunkid, p->goal, quota_exceeded, opflag, &nchunkid);
#endif
	} else {
		bool increaseVersion = (*opflag != 0);
		status = chunk_apply_modification(context.ts(), ochunkid, p->goal, increaseVersion,
				&nchunkid);
	}
	if (status != STATUS_OK) {
		fsnodes_update_checksum(p);
		return status;
	}
	if (context.isPersonalityShadow() && nchunkid != *chunkid) {
		fsnodes_update_checksum(p);
		return ERROR_MISMATCH;
	}
	p->data.fdata.chunktab[indx] = nchunkid;
	*chunkid = nchunkid;
	statsrecord nsr;
	fsnodes_get_stats(p, &nsr);
	for (fsedge* e = p->parents; e; e = e->nextparent) {
		fsnodes_add_sub_stats(e->parent, &nsr, &psr);
	}
	fsnodes_quota_update_size(p, nsr.size - psr.size);
	if (length) {
		*length = p->data.fdata.length;
	}
	if (context.isPersonalityMaster()) {
		fs_changelog(context.ts(),
				"WRITE(%" PRIu32 ",%" PRIu32 ",%" PRIu8 "):%" PRIu64,
				inode, indx, *opflag, nchunkid);
	} else {
		gMetadata->metaversion++;
	}
	if (p->mtime != context.ts() || p->ctime != context.ts()) {
		p->mtime = p->ctime = context.ts();
	}
	fsnodes_update_checksum(p);
#ifndef METARESTORE
	stats_write++;
#endif
	return STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_writeend(uint32_t inode,uint64_t length,uint64_t chunkid) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
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
			fs_changelog(ts, "LENGTH(%" PRIu32 ",%" PRIu64 ")",inode,length);
		}
	}
	fs_changelog(ts, "UNLOCK(%" PRIu64 ")",chunkid);
	return chunk_unlock(chunkid);
}

void fs_incversion(uint64_t chunkid) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fs_changelog(ts, "INCVERSION(%" PRIu64 ")", chunkid);
}
#endif

uint8_t fs_apply_incversion(uint64_t chunkid) {
	gMetadata->metaversion++;
	return chunk_increase_version(chunkid);
}

#ifndef METARESTORE
uint8_t fs_repair(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t *notchanged,uint32_t *erased,uint32_t *repaired) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	uint32_t nversion,indx;
	statsrecord psr,nsr;
	fsedge *e;
	fsnode *p,*rn;

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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
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
			fs_changelog(ts, "REPAIR(%" PRIu32 ",%" PRIu32 "):%" PRIu32,inode,indx,nversion);
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
#endif /* #ifndef METARESTORE */

uint8_t fs_apply_repair(uint32_t ts,uint32_t inode,uint32_t indx,uint32_t nversion) {
	fsnode *p;
	uint8_t status;
	statsrecord psr,nsr;

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
	fsnodes_get_stats(p,&psr);
	if (nversion==0) {
		status = chunk_delete_file(p->data.fdata.chunktab[indx],p->goal);
		p->data.fdata.chunktab[indx]=0;
	} else {
		status = chunk_set_version(p->data.fdata.chunktab[indx],nversion);
	}
	fsnodes_get_stats(p,&nsr);
	for (fsedge *e = p->parents; e; e = e->nextparent) {
		fsnodes_add_sub_stats(e->parent, &nsr, &psr);
	}
	fsnodes_quota_update_size(p, nsr.size - psr.size);
	gMetadata->metaversion++;
	p->mtime = p->ctime = ts;
	fsnodes_update_checksum(p);
	return status;
}

#ifndef METARESTORE
uint8_t fs_getgoal(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t gmode,uint32_t fgtab[10],uint32_t dgtab[10]) {
	fsnode *p,*rn;
	(void)sesflags;
	memset(fgtab,0,10*sizeof(uint32_t));
	memset(dgtab,0,10*sizeof(uint32_t));
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	fsnodes_getgoal_recursive(p,gmode,fgtab,dgtab);
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
				return ERROR_EPERM;
			}
		}
	}
	fsnodes_geteattr_recursive(p,gmode,feattrtab,deattrtab);
	return STATUS_OK;
}

#endif

uint8_t fs_setgoal(const FsContext& context,
		uint32_t inode, uint8_t goal, uint8_t smode,
		uint32_t *sinodes, uint32_t *ncinodes, uint32_t *nsinodes) {
	ChecksumUpdater cu(context.ts());
	if (!SMODE_ISVALID(smode) || goal > 9 || goal < 1) {
		return ERROR_EINVAL;
	}
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kAny);
	if (status != 0) {
		return status;
	}
	fsnode *p;
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kAny, MODE_MASK_EMPTY, inode, &p);
	if (status != STATUS_OK) {
		return status;
	}
	if (p->type != TYPE_DIRECTORY
			&& p->type != TYPE_FILE
			&& p->type != TYPE_TRASH
			&& p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	uint32_t si = 0;
	uint32_t nci = 0;
	uint32_t nsi = 0;
	sassert(context.hasUidGidData());
	fsnodes_setgoal_recursive(p,context.ts(), context.uid(), goal, smode, &si, &nci, &nsi);
	if (context.isPersonalityMaster()) {
		if ((smode & SMODE_RMASK) == 0 && nsi > 0 && si == 0 && nci == 0) {
			return ERROR_EPERM;
		}
		*sinodes = si;
		*ncinodes = nci;
		*nsinodes = nsi;
		fs_changelog(context.ts(),
				"SETGOAL(%" PRIu32 ",%" PRIu32 ",%" PRIu8 ",%" PRIu8 "):%" PRIu32 ",%" PRIu32 ",%" PRIu32,
				p->id, context.uid(), goal, smode, si, nci, nsi);
	} else {
		gMetadata->metaversion++;
		if ((*sinodes != si) || (*ncinodes != nci) || (*nsinodes != nsi)) {
			return ERROR_MISMATCH;
		}
	}
	return STATUS_OK;
}

uint8_t fs_settrashtime(const FsContext& context,
		uint32_t inode, uint32_t trashtime, uint8_t smode,
		uint32_t *sinodes, uint32_t *ncinodes, uint32_t *nsinodes) {
	ChecksumUpdater cu(context.ts());
	if (!SMODE_ISVALID(smode)) {
		return ERROR_EINVAL;
	}
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kAny);
	if (status != 0) {
		return status;
	}
	fsnode *p;
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kAny, MODE_MASK_EMPTY, inode, &p);
	if (status != STATUS_OK) {
		return status;
	}
	if (p->type != TYPE_DIRECTORY
			&& p->type != TYPE_FILE
			&& p->type != TYPE_TRASH
			&& p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	uint32_t si = 0;
	uint32_t nci = 0;
	uint32_t nsi = 0;
	sassert(context.hasUidGidData());
	fsnodes_settrashtime_recursive(p,context.ts(), context.uid(), trashtime, smode, &si, &nci, &nsi);
	if (context.isPersonalityMaster()) {
		if ((smode & SMODE_RMASK) == 0 && nsi > 0 && si == 0 && nci == 0) {
			return ERROR_EPERM;
		}
		*sinodes = si;
		*ncinodes = nci;
		*nsinodes = nsi;
		fs_changelog(context.ts(),
				"SETTRASHTIME(%" PRIu32 ",%" PRIu32 ",%" PRIu32 ",%" PRIu8 "):%" PRIu32 ",%" PRIu32 ",%" PRIu32,
				p->id, context.uid(), trashtime, smode, si, nci, nsi);
	} else {
		gMetadata->metaversion++;
		if ((*sinodes != si) || (*ncinodes != nci) || (*nsinodes != nsi)) {
			return ERROR_MISMATCH;
		}
	}
	return STATUS_OK;
}

uint8_t fs_seteattr(const FsContext& context,
		uint32_t inode, uint8_t eattr, uint8_t smode,
		uint32_t *sinodes, uint32_t *ncinodes, uint32_t *nsinodes) {
	ChecksumUpdater cu(context.ts());
	if (!SMODE_ISVALID(smode)
			|| (eattr & (~(EATTR_NOOWNER|EATTR_NOACACHE|EATTR_NOECACHE|EATTR_NODATACACHE)))) {
		return ERROR_EINVAL;
	}
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != STATUS_OK) {
		return status;
	}
	fsnode *p;
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kAny, MODE_MASK_EMPTY, inode, &p);
	if (status != STATUS_OK) {
		return status;
	}

	uint32_t si = 0;
	uint32_t nci = 0;
	uint32_t nsi = 0;
	sassert(context.hasUidGidData());
	fsnodes_seteattr_recursive(p, context.ts(), context.uid(), eattr, smode, &si, &nci, &nsi);
	if (context.isPersonalityMaster()) {
		if ((smode & SMODE_RMASK) == 0 && nsi > 0 && si == 0 && nci == 0) {
			return ERROR_EPERM;
		}
		*sinodes = si;
		*ncinodes = nci;
		*nsinodes = nsi;
		fs_changelog(context.ts(),
				"SETEATTR(%" PRIu32 ",%" PRIu32 ",%" PRIu8 ",%" PRIu8 "):%" PRIu32 ",%" PRIu32 ",%" PRIu32,
				p->id, context.uid(), eattr, smode, si, nci, nsi);
	} else {
		gMetadata->metaversion++;
		if ((*sinodes != si) || (*ncinodes != nci) || (*nsinodes != nsi)) {
			return ERROR_MISMATCH;
		}
	}
	return STATUS_OK;
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
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
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	fsnode *p,*rn;
	uint8_t status;

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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
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
	if (mode>XATTR_SMODE_REMOVE) {
		return ERROR_EINVAL;
	}
	status = xattr_setattr(inode,anleng,attrname,avleng,attrvalue,mode);
	if (status!=STATUS_OK) {
		return status;
	}
	p->ctime = ts;
	fsnodes_update_checksum(p);
	fs_changelog(ts, "SETXATTR(%" PRIu32 ",%s,%s,%" PRIu8 ")",inode,fsnodes_escape_name(anleng,attrname),fsnodes_escape_name(avleng,attrvalue),mode);
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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
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

#endif /* #ifndef METARESTORE */

uint8_t fs_apply_setxattr(uint32_t ts,uint32_t inode,uint32_t anleng,const uint8_t *attrname,uint32_t avleng,const uint8_t *attrvalue,uint32_t mode) {
	fsnode *p;
	uint8_t status;
	if (anleng==0 || anleng>MFS_XATTR_NAME_MAX || avleng>MFS_XATTR_SIZE_MAX || mode>XATTR_SMODE_REMOVE) {
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
	gMetadata->metaversion++;
	fsnodes_update_checksum(p);
	return status;
}

uint8_t fs_deleteacl(const FsContext& context, uint32_t inode, AclType type) {
	ChecksumUpdater cu(context.ts());
	fsnode *p;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kAny, MODE_MASK_EMPTY, inode, &p);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_deleteacl(p, type, context.ts());
	if (context.isPersonalityMaster()) {
		if (status == STATUS_OK) {
			fs_changelog(context.ts(),
					"DELETEACL(%" PRIu32 ",%c)",
					p->id, (type == AclType::kAccess ? 'a' : 'd'));
		}
	} else {
		gMetadata->metaversion++;
	}
	return status;
}

#ifndef METARESTORE

uint8_t fs_setacl(const FsContext& context, uint32_t inode, AclType type, AccessControlList acl) {
	ChecksumUpdater cu(context.ts());
	fsnode *p;
	uint8_t status = verify_session(context, OperationMode::kReadWrite, SessionType::kNotMeta);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kAny, MODE_MASK_EMPTY, inode, &p);
	if (status != STATUS_OK) {
		return status;
	}
	std::string aclString = acl.toString();
	status = fsnodes_setacl(p, type, std::move(acl), context.ts());
	if (context.isPersonalityMaster()) {
		if (status == STATUS_OK) {
			fs_changelog(context.ts(),
					"SETACL(%" PRIu32 ",%c,%s)",
					p->id, (type == AclType::kAccess ? 'a' : 'd'), aclString.c_str());
		}
	} else {
		gMetadata->metaversion++;
	}
	return status;
}

uint8_t fs_getacl(const FsContext& context, uint32_t inode, AclType type, AccessControlList& acl) {
	fsnode *p;
	uint8_t status = verify_session(context, OperationMode::kReadOnly, SessionType::kAny);
	if (status != STATUS_OK) {
		return status;
	}
	status = fsnodes_get_node_for_operation(context,
			ExpectedNodeType::kAny, MODE_MASK_EMPTY, inode, &p);
	if (status != STATUS_OK) {
		return status;
	}
	return fsnodes_getacl(p, type, acl);
}

#endif /* #ifndef METARESTORE */

uint8_t fs_apply_setacl(uint32_t ts, uint32_t inode, char aclType, const char *aclString) {
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
		gMetadata->metaversion++;
	}
	return status;
}

#ifndef METARESTORE
uint8_t fs_quota_get_all(uint8_t sesflags, uint32_t uid,
		std::vector<QuotaOwnerAndLimits>& results) {
	if (uid != 0 && !(sesflags & SESFLAG_ALLCANCHANGEQUOTA)) {
		return ERROR_EPERM;
	}
	results = gMetadata->gQuotaDatabase.getAll();
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
		const QuotaLimits *result = gMetadata->gQuotaDatabase.get(owner.ownerType, owner.ownerId);
		if (result) {
			tmp.emplace_back(owner, *result);
		}
	}
	results.swap(tmp);
	return STATUS_OK;
}

uint8_t fs_quota_set(uint8_t sesflags, uint32_t uid, const std::vector<QuotaEntry>& entries) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	if (sesflags & SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (uid != 0 && !(sesflags & SESFLAG_ALLCANCHANGEQUOTA)) {
		return ERROR_EPERM;
	}
	for (const QuotaEntry& entry : entries) {
		const QuotaOwner& owner = entry.entryKey.owner;
		gMetadata->gQuotaDatabase.set(entry.entryKey.rigor, entry.entryKey.resource, owner.ownerType,
				owner.ownerId, entry.limit);
		fs_changelog(ts, "SETQUOTA(%c,%c,%c,%" PRIu32 ",%" PRIu64 ")",
				(entry.entryKey.rigor == QuotaRigor::kSoft)? 'S' : 'H',
				(entry.entryKey.resource == QuotaResource::kSize)? 'S' : 'I',
				(owner.ownerType == QuotaOwnerType::kUser)? 'U' : 'G',
				uint32_t{owner.ownerId}, uint64_t{entry.limit});
	}
	return STATUS_OK;
}
#endif

uint8_t fs_apply_setquota(char rigor, char resource, char ownerType, uint32_t ownerId, uint64_t limit) {
	QuotaRigor quotaRigor = QuotaRigor::kSoft;
	QuotaResource quotaResource = QuotaResource::kSize;
	QuotaOwnerType quotaOwnerType = QuotaOwnerType::kUser;
	bool valid = true;
	valid &= decodeChar("SH", {QuotaRigor::kSoft, QuotaRigor::kHard}, rigor, quotaRigor);
	valid &= decodeChar("SI", {QuotaResource::kSize, QuotaResource::kInodes}, resource,
				quotaResource);
	valid &= decodeChar("UG", {QuotaOwnerType::kUser, QuotaOwnerType::kGroup}, ownerType,
				quotaOwnerType);
	if (!valid) {
		return ERROR_EINVAL;
	}
	gMetadata->metaversion++;
	gMetadata->gQuotaDatabase.set(quotaRigor, quotaResource, quotaOwnerType, ownerId, limit);
	return STATUS_OK;
}

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
			if (!fsnodes_isancestor_or_node_reserved_or_trash(rn,p)) {
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
		for (f=gMetadata->nodehash[i] ; f ; f=f->next) {
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
		for (f=gMetadata->nodehash[i] ; f ; f=f->next) {
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
						} else if (vc<f->goal) {
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
				} else if (e->next) {
					if (e->next->prev != &(e->next)) {
						if (leng<MSGBUFFSIZE) {
							leng += fs_test_log_inconsistency(e,"nexthash/prevhash",msgbuff+leng,MSGBUFFSIZE-leng);
						} else {
							fs_test_log_inconsistency(e,"nexthash/prevhash",NULL,0);
						}
					}
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
					} else if (e->next) {
						if (e->next->prev != &(e->next)) {
							if (leng<MSGBUFFSIZE) {
								leng += fs_test_log_inconsistency(e,"nexthash/prevhash",msgbuff+leng,MSGBUFFSIZE-leng);
							} else {
								fs_test_log_inconsistency(e,"nexthash/prevhash",NULL,0);
							}
						}
					}
				}
			}
		}
	}
}
#endif


struct InodeInfo {
	uint32_t free;
	uint32_t reserved;
};

static InodeInfo fs_do_emptytrash(uint32_t ts) {
	fsedge *e;
	fsnode *p;
	InodeInfo ii{0, 0};
	e = gMetadata->trash;
	while (e) {
		p = e->child;
		e = e->nextchild;
		if (((uint64_t)(p->atime) + (uint64_t)(p->trashtime) < (uint64_t)ts) && ((uint64_t)(p->mtime) + (uint64_t)(p->trashtime) < (uint64_t)ts) && ((uint64_t)(p->ctime) + (uint64_t)(p->trashtime) < (uint64_t)ts)) {
			if (fsnodes_purge(ts,p)) {
				ii.free++;
			} else {
				ii.reserved++;
			}
		}
	}
	return ii;
}

#ifndef METARESTORE
static void fs_periodic_emptytrash(void) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	InodeInfo ii = fs_do_emptytrash(ts);
	if (ii.free > 0 || ii.reserved > 0) {
		fs_changelog(ts,
				"EMPTYTRASH():%" PRIu32 ",%" PRIu32,
				ii.free, ii.reserved);
	}
}
#endif

uint8_t fs_apply_emptytrash(uint32_t ts, uint32_t freeinodes, uint32_t reservedinodes) {
	InodeInfo ii = fs_do_emptytrash(ts);
	gMetadata->metaversion++;
	if ((freeinodes != ii.free) || (reservedinodes != ii.reserved)) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
}

static uint32_t fs_do_emptyreserved(uint32_t ts) {
	fsedge *e;
	fsnode *p;
	uint32_t fi = 0;
	e = gMetadata->reserved;
	while (e) {
		p = e->child;
		e = e->nextchild;
		if (p->data.fdata.sessionids==NULL) {
			fsnodes_purge(ts,p);
			fi++;
		}
	}
	return fi;
}

#ifndef METARESTORE
static void fs_periodic_emptyreserved(void) {
	uint32_t ts = main_time();
	ChecksumUpdater cu(ts);
	uint32_t fi = fs_do_emptyreserved(ts);
	if (fi>0) {
		fs_changelog(ts, "EMPTYRESERVED():%" PRIu32,fi);
	}
}
#endif

uint8_t fs_apply_emptyreserved(uint32_t ts,uint32_t freeinodes) {
	uint32_t fi = fs_do_emptyreserved(ts);
	gMetadata->metaversion++;
	if (freeinodes!=fi) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
}

uint64_t fs_getversion() {
	if (!gMetadata) {
		throw NoMetadataException();
	}
	return gMetadata->metaversion;
}

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
		for (p=gMetadata->nodehash[i] ; p ; p=p->next) {
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
	for (n=gMetadata->freelist ; n ; n=n->next) {
		printf("I|i:%10" PRIu32 "|f:%10" PRIu32 "\n",n->id,n->ftime);
	}
}

void xattr_dump() {
	uint32_t i;
	xattr_data_entry *xa;

	for (i=0 ; i<XATTR_DATA_HASH_SIZE ; i++) {
		for (xa=gMetadata->xattr_data_hash[i] ; xa ; xa=xa->next) {
			printf("X|i:%10" PRIu32 "|n:%s|v:%s\n",xa->inode,fsnodes_escape_name(xa->anleng,xa->attrname),fsnodes_escape_name(xa->avleng,xa->attrvalue));
		}
	}
}


void fs_dump(void) {
	fs_dumpnodes();
	fs_dumpedges(gMetadata->root);
	fs_dumpedgelist(gMetadata->trash);
	fs_dumpedgelist(gMetadata->reserved);
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
		for (xa=gMetadata->xattr_data_hash[i] ; xa ; xa=xa->next) {
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
		for (ih = gMetadata->xattr_inode_hash[ihash]; ih && ih->inode!=inode; ih=ih->next) {}

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

		xa = new xattr_data_entry;
		xa->inode = inode;
		xa->attrname = (uint8_t*) malloc(anleng);
		passert(xa->attrname);
		if (fread(xa->attrname,1,anleng,fd)!=(size_t)anleng) {
			int err = errno;
			if (nl) {
				fputc('\n',stderr);
			}
			delete xa;
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
				delete xa;
				errno = err;
				mfs_errlog(LOG_ERR,"loading xattr: read error");
				return -1;
			}
		} else {
			xa->attrvalue = NULL;
		}
		xa->avleng = avleng;
		hash = xattr_data_hash_fn(inode,xa->anleng,xa->attrname);
		xa->next = gMetadata->xattr_data_hash[hash];
		if (xa->next) {
			xa->next->prev = &(xa->next);
		}
		xa->prev = gMetadata->xattr_data_hash + hash;
		gMetadata->xattr_data_hash[hash] = xa;

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
			ih->next = gMetadata->xattr_inode_hash[ihash];
			gMetadata->xattr_inode_hash[ihash] = ih;
		}
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
	uint32_t hpos;
	fsedge *e;
	statsrecord sr;
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
	e = new fsedge;
	e->nleng = get16bit(&ptr);
	if (e->nleng==0) {
		if (nl) {
			fputc('\n',stderr);
			nl=0;
		}
		mfs_arg_syslog(LOG_ERR,"loading edge: %" PRIu32 "->%" PRIu32 " error: empty name",parent_id,child_id);
		delete e;
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
		delete e;
		return -1;
	}
	e->child = fsnodes_id_to_node(child_id);
	if (e->child==NULL) {
		if (nl) {
			fputc('\n',stderr);
			nl=0;
		}
		mfs_arg_syslog(LOG_ERR,"loading edge: %" PRIu32 ",%s->%" PRIu32 " error: child not found",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
		delete e;
		if (ignoreflag) {
			return 0;
		}
		return -1;
	}
	if (parent_id==0) {
		if (e->child->type==TYPE_TRASH) {
			e->parent = NULL;
			e->nextchild = gMetadata->trash;
			if (e->nextchild) {
				e->nextchild->prevchild = &(e->nextchild);
			}
			gMetadata->trash = e;
			e->prevchild = &gMetadata->trash;
			e->next = NULL;
			e->prev = NULL;
			gMetadata->trashspace += e->child->data.fdata.length;
			gMetadata->trashnodes++;
		} else if (e->child->type==TYPE_RESERVED) {
			e->parent = NULL;
			e->nextchild = gMetadata->reserved;
			if (e->nextchild) {
				e->nextchild->prevchild = &(e->nextchild);
			}
			gMetadata->reserved = e;
			e->prevchild = &gMetadata->reserved;
			e->next = NULL;
			e->prev = NULL;
			gMetadata->reservedspace += e->child->data.fdata.length;
			gMetadata->reservednodes++;
		} else {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			mfs_arg_syslog(LOG_ERR,
					"loading edge: %" PRIu32 ",%s->%" PRIu32 " error: bad child type (%c)\n",
					parent_id, fsnodes_escape_name(e->nleng, e->name), child_id, e->child->type);
			delete e;
			return -1;
		}
	} else {
		e->parent = fsnodes_id_to_node(parent_id);
		if (e->parent==NULL) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			mfs_arg_syslog(LOG_ERR,
					"loading edge: %" PRIu32 ",%s->%" PRIu32 " error: parent not found",
					parent_id, fsnodes_escape_name(e->nleng, e->name), child_id);
			if (ignoreflag) {
				e->parent = fsnodes_id_to_node(MFS_ROOT_ID);
				if (e->parent==NULL || e->parent->type!=TYPE_DIRECTORY) {
					mfs_arg_syslog(LOG_ERR,
							"loading edge: %" PRIu32 ",%s->%" PRIu32 " root dir not found !!!",
							parent_id, fsnodes_escape_name(e->nleng, e->name), child_id);
					delete e;
					return -1;
				}
				mfs_arg_syslog(LOG_ERR,
						"loading edge: %" PRIu32 ",%s->%" PRIu32 " attaching node to root dir",
						parent_id, fsnodes_escape_name(e->nleng, e->name), child_id);
				parent_id = MFS_ROOT_ID;
			} else {
				mfs_syslog(LOG_ERR,
						"use mfsmetarestore (option -i) to attach this node to root dir\n");
				delete e;
				return -1;
			}
		}
		if (e->parent->type!=TYPE_DIRECTORY) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			mfs_arg_syslog(LOG_ERR,
					"loading edge: %" PRIu32 ",%s->%" PRIu32 " error: bad parent type (%c)",
					parent_id, fsnodes_escape_name(e->nleng, e->name), child_id, e->parent->type);
			if (ignoreflag) {
				e->parent = fsnodes_id_to_node(MFS_ROOT_ID);
				if (e->parent==NULL || e->parent->type!=TYPE_DIRECTORY) {
					mfs_arg_syslog(LOG_ERR,
							"loading edge: %" PRIu32 ",%s->%" PRIu32 " root dir not found !!!",
							parent_id, fsnodes_escape_name(e->nleng, e->name), child_id);
					delete e;
					return -1;
				}
				mfs_arg_syslog(LOG_ERR,
						"loading edge: %" PRIu32 ",%s->%" PRIu32 " attaching node to root dir",
						parent_id, fsnodes_escape_name(e->nleng, e->name), child_id);
				parent_id = MFS_ROOT_ID;
			} else {
				mfs_syslog(LOG_ERR,
						"use mfsmetarestore (option -i) to attach this node to root dir\n");
				delete e;
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
				syslog(LOG_ERR,
						"loading edge: %" PRIu32 ",%s->%" PRIu32 " error: parent node sequence error",
						parent_id, fsnodes_escape_name(e->nleng, e->name), child_id);
				if (ignoreflag) {
					current_tail = &(e->parent->data.ddata.children);
					while (*current_tail) {
						current_tail = &((*current_tail)->nextchild);
					}
				} else {
					delete e;
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
		hpos = EDGEHASHPOS(fsnodes_hash(e->parent->id,e->nleng,e->name));
		e->next = gMetadata->edgehash[hpos];
		if (e->next) {
			e->next->prev = &(e->next);
		}
		gMetadata->edgehash[hpos] = e;
		e->prev = &(gMetadata->edgehash[hpos]);
	}
	e->nextparent = e->child->parents;
	if (e->nextparent) {
		e->nextparent->prevparent = &(e->nextparent);
	}
	e->child->parents = e;
	e->prevparent = &(e->child->parents);
	if (e->parent) {
		fsnodes_get_stats(e->child,&sr);
		fsnodes_add_stats(e->parent,&sr);
	}
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
	statsrecord *sr;
	static uint8_t nl;

	if (fd==NULL) {
		nl=1;
		return 0;
	}

	type = fgetc(fd);
	if (type==0) {  // last node
		return 1;
	}
	p = new fsnode(type);
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
		sr = (statsrecord*) malloc(sizeof(statsrecord));
		passert(sr);
		memset(sr,0,sizeof(statsrecord));
		p->data.ddata.stats = sr;
		p->data.ddata.children = NULL;
		p->data.ddata.nlink = 2;
		p->data.ddata.elements = 0;
		break;
	case TYPE_SOCKET:
	case TYPE_FIFO:
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		p->data.devdata.rdev = get32bit(&ptr);
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
			matoclserv_add_open_file(sessionid,p->id);
#endif
			sessionids--;
		}
		fsnodes_quota_update_size(p, +fsnodes_get_size(p));
	}
	p->parents = NULL;
	nodepos = NODEHASHPOS(p->id);
	p->next = gMetadata->nodehash[nodepos];
	gMetadata->nodehash[nodepos] = p;
	fsnodes_used_inode(p->id);
	gMetadata->nodes++;
	if (type==TYPE_DIRECTORY) {
		gMetadata->dirnodes++;
	}
	if (type==TYPE_FILE || type==TYPE_TRASH || type==TYPE_RESERVED) {
		gMetadata->filenodes++;
	}
	fsnodes_quota_register_inode(p);
	return 0;
}

void fs_storenodes(FILE *fd) {
	uint32_t i;
	fsnode *p;
	for (i=0 ; i<NODEHASHSIZE ; i++) {
		for (p=gMetadata->nodehash[i] ; p ; p=p->next) {
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
	fs_storeedges_rec(gMetadata->root,fd);
	fs_storeedgelist(gMetadata->trash,fd);
	fs_storeedgelist(gMetadata->reserved,fd);
	fs_storeedge(NULL,fd);  // end marker
}

static void fs_storeacls(FILE *fd) {
	for (uint32_t i = 0; i < NODEHASHSIZE; ++i) {
		for (fsnode *p = gMetadata->nodehash[i]; p; p = p->next) {
			if (p->extendedAcl || p->defaultAcl) {
				fs_storeacl(p, fd);
			}
		}
	}
	fs_storeacl(nullptr, fd); // end marker
}

static void fs_storequotas(FILE *fd) {
	const std::vector<QuotaEntry>& entries = gMetadata->gQuotaDatabase.getEntries();
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
		if (!fsnodes_nameisused(gMetadata->root,l,artname)) {
			fsnodes_link(0,gMetadata->root,p,l,artname);
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
		for (p=gMetadata->nodehash[i] ; p ; p=p->next) {
			if (p->parents==NULL && p!=gMetadata->root) {
				if (nl) {
					fputc('\n',stderr);
					nl=0;
				}
				mfs_arg_syslog(LOG_ERR, "found orphaned inode: %" PRIu32, p->id);
				if (ignoreflag) {
					if (fs_lostnode(p)<0) {
						return -1;
					}
				} else {
					mfs_syslog(LOG_ERR,
							"use mfsmetarestore (option -i) to attach this node to root dir\n");
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
			gMetadata->gQuotaDatabase.set(entry.entryKey.rigor, entry.entryKey.resource,
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
	for (n=gMetadata->freelist ; n ; n=n->next) {
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
	for (n=gMetadata->freelist ; n ; n=n->next) {
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
	gMetadata->freelist = NULL;
	gMetadata->freetail = &(gMetadata->freelist);
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
		*gMetadata->freetail = n;
		gMetadata->freetail = &(n->next);
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
	put32bit(&ptr,gMetadata->maxnodeid);
	put64bit(&ptr,gMetadata->metaversion);
	put32bit(&ptr,gMetadata->nextsessionid);
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
#if LIZARDFS_VERSHEX >= LIZARDFS_VERSION(1, 6, 29)
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
		mfs_syslog(LOG_ERR, "error loading header\n");
		return -1;
	}
	ptr = hdr;
	gMetadata->maxnodeid = get32bit(&ptr);
	gMetadata->metaversion = get64bit(&ptr);
	gMetadata->nextsessionid = get32bit(&ptr);
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
		if (chunk_load(fd)<0) {
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
				if (chunk_load(fd)<0) {
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
	gMetadata->root = fsnodes_id_to_node(MFS_ROOT_ID);
	if (gMetadata->root==NULL) {
		mfs_syslog(LOG_ERR, "error reading metadata (root node not found !!!)");
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
	gMetadata->maxnodeid = MFS_ROOT_ID;
	gMetadata->metaversion = 1;
	gMetadata->nextsessionid = 1;
	fsnodes_init_freebitmask();
	gMetadata->freelist = NULL;
	gMetadata->freetail = &(gMetadata->freelist);
	gMetadata->root = new fsnode(TYPE_DIRECTORY);
	gMetadata->root->id = MFS_ROOT_ID;
	gMetadata->root->ctime = gMetadata->root->mtime = gMetadata->root->atime = main_time();
	gMetadata->root->goal = DEFAULT_GOAL;
	gMetadata->root->trashtime = DEFAULT_TRASHTIME;
	gMetadata->root->mode = 0777;
	gMetadata->root->uid = 0;
	gMetadata->root->gid = 0;
	sr = (statsrecord*) malloc(sizeof(statsrecord));
	passert(sr);
	memset(sr,0,sizeof(statsrecord));
	gMetadata->root->data.ddata.stats = sr;
	gMetadata->root->data.ddata.children = NULL;
	gMetadata->root->data.ddata.elements = 0;
	gMetadata->root->data.ddata.nlink = 2;
	gMetadata->root->parents = NULL;
	nodepos = NODEHASHPOS(gMetadata->root->id);
	gMetadata->root->next = gMetadata->nodehash[nodepos];
	gMetadata->nodehash[nodepos] = gMetadata->root;
	fsnodes_used_inode(gMetadata->root->id);
	chunk_newfs();
	gMetadata->nodes=1;
	gMetadata->dirnodes=1;
	gMetadata->filenodes=0;
	fs_checksum(ChecksumMode::kForceRecalculate);
	fsnodes_quota_register_inode(gMetadata->root);
}
#endif

int fs_emergency_storeall(const std::string& fname) {
	cstream_t fd(fopen(fname.c_str(), "w"));
	if (fd == nullptr) {
		return -1;
	}

	fs_store_fd(fd.get());

	if (ferror(fd.get())!=0) {
		return -1;
	}
	syslog(LOG_WARNING,
			"metadata were stored to emergency file: %s - please copy this file to your default location as '%s'",
			fname.c_str(), kMetadataFilename);
	return 0;
}

int fs_emergency_saves() {
#if defined(LIZARDFS_HAVE_PWD_H) && defined(LIZARDFS_HAVE_GETPWUID)
	struct passwd *p;
#endif
	if (fs_emergency_storeall(kMetadataEmergencyFilename) == 0) {
		return 0;
	}
#if defined(LIZARDFS_HAVE_PWD_H) && defined(LIZARDFS_HAVE_GETPWUID)
	p = getpwuid(getuid());
	if (p) {
		std::string fname = p->pw_dir;
		fname.append("/").append(kMetadataEmergencyFilename);
		if (fs_emergency_storeall(fname) == 0) {
			return 0;
		}
	}
#endif
	std::string metadata_emergency_filename = kMetadataEmergencyFilename;
	if (fs_emergency_storeall("/" + metadata_emergency_filename) == 0) {
		return 0;
	}
	if (fs_emergency_storeall("/tmp/" + metadata_emergency_filename) == 0) {
		return 0;
	}
	if (fs_emergency_storeall("/var/" + metadata_emergency_filename) == 0) {
		return 0;
	}
	if (fs_emergency_storeall("/usr/" + metadata_emergency_filename) == 0) {
		return 0;
	}
	if (fs_emergency_storeall("/usr/share/" + metadata_emergency_filename) == 0) {
		return 0;
	}
	if (fs_emergency_storeall("/usr/local/" + metadata_emergency_filename) == 0) {
		return 0;
	}
	if (fs_emergency_storeall("/usr/local/var/" + metadata_emergency_filename) == 0) {
		return 0;
	}
	if (fs_emergency_storeall("/usr/local/share/" + metadata_emergency_filename) == 0) {
		return 0;
	}
	return -1;
}

void fs_unlock() {
	gMetadataLockfile->unlock();
}

#ifndef METARESTORE
// returns false in case of an error
bool fs_storeall(MetadataDumper::DumpType dumpType) {
	if (gMetadata == nullptr) {
		// Periodic dump in shadow master or a reload with DUMP_METADATA_ON_RELOAD
		syslog(LOG_INFO, "Can't save metadata because no metadata is loaded");
		return false;
	}
	if (metadataDumper.inProgress()) {
		syslog(LOG_ERR, "previous metadata save process hasn't finished yet - do not start another one");
		return false;
	}
	changelog_rotate();
	// child == true says that we forked
	// bg may be changed to dump in foreground in case of a fork error
	bool child = metadataDumper.start(dumpType, fs_checksum(ChecksumMode::kGetCurrent));

	if (dumpType == MetadataDumper::kForegroundDump) {
		cstream_t fd(fopen(kMetadataTmpFilename, "w"));
		if (fd == nullptr) {
			syslog(LOG_ERR, "can't open metadata file");
			// try to save in alternative location - just in case
			fs_emergency_saves();
			if (child) {
				exit(1);
			}
			return false;
		}

		fs_store_fd(fd.get());

		if (ferror(fd.get()) != 0) {
			syslog(LOG_ERR, "can't write metadata");
			fd.reset();
			unlink(kMetadataTmpFilename);
			// try to save in alternative location - just in case
			fs_emergency_saves();
			if (child) {
				exit(1);
			}
			return false;
		} else {
			if (fflush(fd.get()) == EOF) {
				mfs_errlog(LOG_ERR, "metadata fflush failed");
			} else if (fsync(fileno(fd.get())) == -1) {
				mfs_errlog(LOG_ERR, "metadata fsync failed");
			}
			fd.reset();
			if (!child) {
				// rename backups if no child was created, otherwise this is handled by pollServe
				rotateFiles(kMetadataTmpFilename, kMetadataFilename, gStoredPreviousBackMetaCopies);
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

void fs_periodic_storeall() {
	fs_storeall(MetadataDumper::kBackgroundDump); // ignore error
}

void fs_term(void) {
	if (metadataDumper.inProgress()) {
		metadataDumper.waitUntilFinished();
	}
	for (;;) {
		if (gMetadata == nullptr || fs_storeall(MetadataDumper::kForegroundDump)) {
			break;
		}
		syslog(LOG_ERR,"can't store metadata - try to make more space on your hdd or change privieleges - retrying after 10 seconds");
		sleep(10);
	}
	chunk_unload();
	// delete gMetadata; // We may do this, but it would slow down restarts of the master server
	fs_unlock();
}

#else
void fs_storeall(const char *fname) {
	FILE *fd;
	fd = fopen(fname,"w");
	if (fd==NULL) {
		mfs_syslog(LOG_ERR, "can't open metadata file");
		return;
	}
	fs_store_fd(fd);

	if (ferror(fd)!=0) {
		mfs_syslog(LOG_ERR, "can't write metadata\n");
	} else if (fflush(fd) == EOF) {
		mfs_syslog(LOG_ERR, "can't fflush metadata\n");
	} else if (fsync(fileno(fd)) == -1) {
		mfs_syslog(LOG_ERR, "can't fsync metadata\n");
	}
	fclose(fd);
}

void fs_term(const char *fname, bool noLock) {
	fs_storeall(fname);
	if (!noLock) {
		fs_unlock();
	}
}
#endif

LIZARDFS_CREATE_EXCEPTION_CLASS(MetadataException, Exception);
LIZARDFS_CREATE_EXCEPTION_CLASS(MetadataFsConsistencyException, MetadataException);
LIZARDFS_CREATE_EXCEPTION_CLASS(MetadataConsistencyException, MetadataException);
char const MetadataStructureReadErrorMsg[] = "error reading metadata (structure)";

void fs_loadall(const std::string& fname,int ignoreflag) {
	cstream_t fd(fopen(fname.c_str(), "r"));
	if (fd == nullptr) {
		throw FilesystemException("can't open metadata file: " + errorString(errno));
	}
	uint8_t hdr[8];
	if (fread(hdr,1,8,fd.get())!=8) {
		throw MetadataConsistencyException("can't read metadata header");
	}
#ifndef METARESTORE
	if (metadataserver::isMaster()) {
		if (memcmp(hdr, "MFSM NEW", 8) == 0) {    // special case - create new file system
			mfs_syslog(LOG_NOTICE, "create new empty filesystem");
			fs_new();
			// after creating new filesystem always create "back" file for using in metarestore
			fs_storeall(MetadataDumper::kForegroundDump);
			return;
		}
	}
#endif /* #ifndef METARESTORE */
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
	if (fs_load(fd.get(), ignoreflag, metadataVersion) < 0) {
		throw MetadataConsistencyException(MetadataStructureReadErrorMsg);
	}
	if (ferror(fd.get())!=0) {
		throw MetadataConsistencyException(MetadataStructureReadErrorMsg);
	}
	fprintf(stderr,"connecting files and chunks ... ");
	fflush(stderr);
	fs_add_files_to_chunks();
	fprintf(stderr,"ok\n");
#ifndef METARESTORE
	fprintf(stderr,"all inodes: %" PRIu32 "\n",gMetadata->nodes);
	fprintf(stderr,"directory inodes: %" PRIu32 "\n",gMetadata->dirnodes);
	fprintf(stderr,"file inodes: %" PRIu32 "\n",gMetadata->filenodes);
	fprintf(stderr,"chunks: %" PRIu32 "\n",chunk_count());
#endif
	unlink(kMetadataTmpFilename);
	fs_checksum(ChecksumMode::kForceRecalculate);
	return;
}

void fs_strinit(void) {
	gMetadata = new FilesystemMetadata;
}

/* executed in master mode */
#ifndef METARESTORE
int fs_loadall(void) {
	fprintf(stderr,"loading metadata ...\n");
	fs_strinit();
	chunk_strinit();
	changelogsMigrateFrom_1_6_29("changelog");
	namespace fs = boost::filesystem;
	if (fs::exists(kMetadataTmpFilename)) {
		throw MetadataFsConsistencyException(
				"temporary metadata file exists, metadata directory is in dirty state");
	}
	if ((metadataserver::isMaster()) && !fs::exists(kMetadataFilename)) {
		std::string currentPath(fs::current_path().string());
		fprintf(stderr, "Can't open metadata file: If this is new instalation "
			"then rename %s/%s.empty as %s/%s\n",
			currentPath.c_str(), kMetadataFilename,
			currentPath.c_str(), kMetadataFilename);
	}
	fs_loadall(kMetadataFilename, 0);
	fprintf(stderr,"metadata file has been loaded\n");

	if (gAutoRecovery || (metadataserver::getPersonality() == metadataserver::Personality::kShadow)) {
		int err = fs_load_changelogs();
		if (err != 0) {
			return err;
		}
	}
	return 0;
}

void fs_cs_disconnected(void) {
	test_start_time = main_time()+600;
}

/*
 * Initialize subsystems required by Master personality of metadataserver.
 */
void fs_become_master() {
	dcm_clear();
	test_start_time = main_time() + 900;
	main_timeregister(TIMEMODE_RUN_LATE, 1, 0, fs_test_files);
	gEmptyTrashHook = main_timeregister(TIMEMODE_RUN_LATE,
			cfg_get_minvalue<uint32_t>("EMPTY_TRASH_PERIOD", 300, 1),
			0, fs_periodic_emptytrash);
	gEmptyReservedHook = main_timeregister(TIMEMODE_RUN_LATE,
			cfg_get_minvalue<uint32_t>("EMPTY_RESERVED_INODES_PERIOD", 60, 1),
			0, fs_periodic_emptyreserved);
	gFreeInodesHook = main_timeregister(TIMEMODE_RUN_LATE,
			cfg_get_minvalue<uint32_t>("FREE_INODES_PERIOD", 60, 1),
			0, fs_periodic_freeinodes);
	ChecksumUpdater::setPeriod(cfg_getint32("METADATA_CHECKSUM_INTERVAL", 50));
	return;
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
		fs_storeall(MetadataDumper::kBackgroundDump);
	}

	if (metadataserver::isDuringPersonalityChange()) {
		if (!gMetadata) {
			syslog(LOG_ERR, "Attempted shadow->master transition without metadata - aborting");
			exit(1);
		}
		fs_become_master();
	} else if (metadataserver::isMaster()) {
		main_timechange(gEmptyTrashHook, TIMEMODE_RUN_LATE,
				cfg_get_minvalue<uint32_t>("EMPTY_TRASH_PERIOD", 300, 1), 0);
		main_timechange(gEmptyReservedHook, TIMEMODE_RUN_LATE,
				cfg_get_minvalue<uint32_t>("EMPTY_RESERVED_INODES_PERIOD", 60, 1), 0);
		main_timechange(gFreeInodesHook, TIMEMODE_RUN_LATE,
				cfg_get_minvalue<uint32_t>("FREE_INODES_PERIOD", 60, 1), 0);
		ChecksumUpdater::setPeriod(cfg_getint32("METADATA_CHECKSUM_INTERVAL", 50));
	}
}

/*
 * Load and apply given changelog file.
 */
void fs_load_changelog(const std::string& path) {
	std::ifstream changelog(path);
	std::string line;
	size_t end = 0;
	sassert(gMetadata->metaversion > 0);

	uint64_t first = 0;
	uint64_t id = 0;
	uint32_t skippedEntries = 0;
	while (std::getline(changelog, line).good()) {
		id = stoull(line, &end);
		if (id < fs_getversion()) {
			++skippedEntries;
			continue;
		} else if (!first) {
			first = id;
		}
		if (restore(path.c_str(), id, line.c_str() + end, RestoreRigor::kIgnoreParseErrors) < 0) {
			throw MetadataConsistencyException("Can't apply changelog " + path);
		}
	}
	if (id >= first) {
		mfs_arg_syslog(LOG_NOTICE, "applied changes from %" PRIu64 " to %" PRIu64 " from %s",
				first, id, path.c_str());
	} else {
		mfs_arg_syslog(LOG_NOTICE, "no changes applied from %s", path.c_str());
	}
	if (skippedEntries > 0) {
		mfs_arg_syslog(LOG_NOTICE, "skipped %" PRIu32 " entries from changelog %s",
				skippedEntries, path.c_str());
	}
}

/*
 * Load and apply changelogs.
 */
int fs_load_changelogs() {
	metadataserver::Personality personality = metadataserver::getPersonality();
	metadataserver::setPersonality(metadataserver::Personality::kShadow);
	/*
	 * We need to load 3 changelog files in extreme case.
	 * If we are being run as Shadow we need to download two
	 * changelog files:
	 * 1 - current changelog => "changelog.mfs.1"
	 * 2 - previous changelog in case Shadow connects during metadata dump,
	 *     that is "changelog.mfs.2"
	 * Beside this we received changelog lines that we stored in
	 * yet another changelog file => "changelog.mfs"
	 *
	 * If we are master we only really care for:
	 * "changelog.mfs.1" and "changelog.mfs" files.
	 */
	static const std::string changelogs[] {
		std::string(kChangelogFilename) + ".2",
		std::string(kChangelogFilename) + ".1",
		kChangelogFilename
	};
	restore_setverblevel(gVerbosity);
	namespace fs = boost::filesystem;
	bool oldExists = false;
	try {
		for (const std::string& s : changelogs) {
			if (fs::exists(s)) {
				oldExists = true;
				mfs_arg_syslog(LOG_NOTICE, "Changelog file found and %s, trying to apply %s.",
						(metadataserver::isMaster() ? "auto recovery enabled" : "running as Shadow"),
						s.c_str());
				uint64_t first = changelogGetFirstLogVersion(s);
				uint64_t last = changelogGetLastLogVersion(s);
				if (last >= first) {
					if (last >= fs_getversion()) {
						fs_load_changelog(s);
						mfs_arg_syslog(LOG_NOTICE, "Changelog file %s applied successfully.", s.c_str());
					} else {
						mfs_arg_syslog(LOG_WARNING,
								"Skipping changelog file: %s [%" PRIu64 "-%" PRIu64 "]",
								s.c_str(), fs_getversion(), last);
					}
				} else {
					mfs_arg_syslog(LOG_ERR,
							"Changelog inconsistent, run meterestore,"
							" current version = %" PRIu64 ", new version = %" PRIu64 ".",
							fs_getversion(), first);
					return -1;
				}
			} else if (oldExists) {
				mfs_arg_syslog(LOG_WARNING, "missing changelog file `%s'", s.c_str());
			}
		}
	} catch (fs::filesystem_error& ex) {
		throw FilesystemException("Error loading changelogs" + std::string(ex.what()));
	}
	fs_checksum(ChecksumMode::kForceRecalculate);
	fs_storeall(MetadataDumper::DumpType::kForegroundDump);
	metadataserver::setPersonality(personality);
	return 0;
}

void fs_unload() {
	mfs_arg_syslog(LOG_WARNING, "unloading filesystem at %" PRIu64, fs_getversion());
	restore_reset();
	matoclserv_session_unload();
	chunk_unload();
	dcm_clear();
	delete gMetadata;
	gMetadata = nullptr;
}

int fs_init(bool doLoad) {
	if (!gMetadataLockfile) {
		gMetadataLockfile.reset(new Lockfile(kMetadataFilename + std::string(".lock")));
	}
	gAutoRecovery = cfg_getint32("AUTO_RECOVERY", 0) == 1 ? true : false;
	if (!gMetadataLockfile->isLocked()) {
		try {
			gMetadataLockfile->lock((gAutoRecovery || !metadataserver::isMaster()) ?
					Lockfile::StaleLock::kSwallow : Lockfile::StaleLock::kReject);
		} catch (const LockfileException& e) {
			if (e.reason() == LockfileException::Reason::kStaleLock) {
				throw LockfileException(
						std::string(e.what()) + ", consider running `mfsmetarestore -a' to fix problems with your datadir.",
						LockfileException::Reason::kStaleLock);
			}
			throw;
		}
	}
	ChecksumUpdater::setPeriod(cfg_getint32("METADATA_CHECKSUM_INTERVAL", 50));
	gStoredPreviousBackMetaCopies = cfg_get_maxvalue(
			"BACK_META_KEEP_PREVIOUS",
			kDefaultStoredPreviousBackMetaCopies,
			kMaxStoredPreviousBackMetaCopies);
	if (doLoad || (metadataserver::isMaster())) {
		int err = fs_loadall();
		if (err != 0) {
			return err;
		}
	}
	metadataDumper.setMetarestorePath(cfg_get("MFSMETARESTORE_PATH",
			std::string(SBIN_PATH "/mfsmetarestore")));
	metadataDumper.setUseMetarestore(cfg_getint32("PREFER_BACKGROUND_DUMP", 0));

	main_reloadregister(fs_reload);
	if (!cfg_isdefined("MAGIC_DISABLE_METADATA_DUMPS")) {
		// Secret option disabling periodic metadata dumps
		main_timeregister(TIMEMODE_RUN_LATE,3600,0,fs_periodic_storeall);
	}
	if (metadataserver::isMaster()) {
		fs_become_master();
	}
	main_pollregister(metadataPollDesc, metadataPollServe);
	main_destructregister(fs_term);
	return 0;
}

/*
 * Initialize filesystem subsystem if currently metadataserver have Master personality.
 */
int fs_init() {
	return fs_init(false);
}

#else
int fs_init(const char *fname,int ignoreflag, bool noLock) {
	if (!noLock) {
		boost::filesystem::path path(fname);
		gMetadataLockfile.reset(new Lockfile(
					(path.parent_path() / (kMetadataFilename + std::string(".lock"))).string()));
		gMetadataLockfile->lock(Lockfile::StaleLock::kSwallow);
	}
	fs_strinit();
	chunk_strinit();
	fs_loadall(fname,ignoreflag);
	return 0;
}
#endif
