/*
   Copyright 2008 Gemius SA.

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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <inttypes.h>
#include <errno.h>

#include "MFSCommunication.h"

#ifndef METARESTORE
#include "matocuserv.h"
#include "matocsserv.h"
#endif

#include "chunks.h"
#include "filesystem.h"
#include "datapack.h"

#ifndef METARESTORE
#include "main.h"
#include "changelog.h"
#endif

#define EDGEHASH 1
#define BACKGROUND_METASTORE 1

#define NODEHASHBITS (16)
#define NODEHASHSIZE (1<<NODEHASHBITS)
#define NODEHASHPOS(nodeid) ((nodeid)&(NODEHASHSIZE-1))

#ifdef EDGEHASH
#define EDGEHASHBITS (20)
#define EDGEHASHSIZE (1<<EDGEHASHBITS)
#define EDGEHASHPOS(hash) ((hash)&(EDGEHASHSIZE-1))
#define LOOKUPNOHASHLIMIT 10
#endif

//#define GOAL(x) ((x)&0xF)
//#define DELETE(x) (((x)>>4)&1)
//#define SETGOAL(x,y) ((x)=((x)&0xF0)|((y)&0xF))
//#define SETDELETE(x,y) ((x)=((x)&0xF)|((y)&0x10))
#define DEFAULT_GOAL 1
#define DEFAULT_TRASHTIME 86400

#define MAXFNAMELENG 255

#define MAX_INDEX 0x7FFF
#define MAX_CHUNKS_PER_FILE (MAX_INDEX+1)

#ifndef METARESTORE
typedef struct _bstnode {
	uint32_t val,count;
	struct _bstnode *left,*right;
} bstnode;
#endif

typedef struct _cuidrec {
	uint32_t cuid;
	struct _cuidrec *next;
} cuidrec;

struct _fsnode;

typedef struct _fsedge {
	struct _fsnode *child,*parent;
	struct _fsedge *nextchild,*nextparent;
	struct _fsedge **prevchild,**prevparent;
#ifdef EDGEHASH
	struct _fsedge *next,**prev;
#endif
	uint16_t nleng;
	uint8_t *name;
} fsedge;

typedef struct _fsnode {
	uint32_t id;
	uint32_t ctime,mtime,atime;
	uint8_t type;
	uint8_t goal;
	uint16_t mode;
	uint32_t uid;
	uint32_t gid;
	uint32_t trashtime;
	union _data {
		struct _ddata {				// type==TYPE_DIRECTORY
			fsedge *children;
			uint32_t nlink;
			uint32_t elements;
//			struct _fsnode parent;
			//uint64_t length;		// sum of file lengths
		} ddata;
		struct _sdata {				// type==TYPE_SYMLINK
			uint32_t pleng;
			uint8_t *path;
		} sdata;
		uint32_t rdev;				// type==TYPE_BLOCKDEV ; type==TYPE_CHARDEV
		struct _fdata {				// type==TYPE_FILE
			uint64_t length;
			uint64_t *chunktab;
			uint32_t chunks;
			cuidrec *cuids;
//			uint32_t pleng;			// path to undelete - used in trash only
//			uint8_t *path;			// path to undelete - used in trash only
		} fdata;
	} data;
	fsedge *parents;
	struct _fsnode *next;
} fsnode;

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
static uint32_t nextcuid;
static uint32_t nodes;

static uint64_t version;
static uint64_t trashspace;
static uint64_t reservedspace;
static uint32_t trashnodes;
static uint32_t reservednodes;
static uint32_t filenodes;
static uint32_t dirnodes;

#ifndef METARESTORE

#define MSGBUFFSIZE 100000

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

static uint32_t starttime;

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

#endif

uint32_t fsnodes_get_next_id() {
	uint32_t i,mask;
	while (searchpos<bitmasksize && freebitmask[searchpos]==0xFFFFFFFF) {
		searchpos++;
	}
	if (searchpos==bitmasksize) {	// no more freeinodes
		bitmasksize+=0x80;
		freebitmask = (uint32_t*)realloc(freebitmask,bitmasksize*sizeof(uint32_t));
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
	n = (freenode*)malloc(sizeof(freenode));
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
		free(n);
		n = an;
	}
	if (n) {
		freelist = n;
	} else {
		freelist = NULL;
		freetail = &(freelist);
	}
#ifndef METARESTORE
	changelog(version++,"%u|FREEINODES():%u",main_time(),fi);
#else
	version++;
	if (freeinodes!=fi) {
		return 1;
	}
	return 0;
#endif
}

void fsnodes_init_freebitmask (void) {
	bitmasksize = 0x100+(((maxnodeid)>>5)&0xFFFFFF80);
	freebitmask = (uint32_t*)malloc(bitmasksize*sizeof(uint32_t));
	memset(freebitmask,0,bitmasksize*sizeof(uint32_t));
	freebitmask[0]=1;	// reserve inode 0
	searchpos = 0;
}

void fsnodes_used_inode (uint32_t id) {
	uint32_t pos,mask;
	pos = id>>5;
	mask = 1<<(id&0x1F);
	freebitmask[pos]|=mask;
}


/*
char* fsnodes_escape_name(uint16_t nleng,const uint8_t *name) {
	static uint8_t escname[3*MAXFNAMELENG+1];
	uint32_t i;
	uint8_t c;
	i = 0;
	while (nleng>0) {
		c=*name++;
		if (c<32 || c>127 || c==',' || c=='%' || c=='(' || c==')' || c==0) {
			escname[i++]='%';
			escname[i++]="0123456789ABCDEF"[(c>>4)&0xF];
			escname[i++]="0123456789ABCDEF"[c&0xF];
		} else {
			escname[i++]=c;
		}
		nleng--;
	}
	escname[i]=0;
	return (char*)escname;
}
*/

char* fsnodes_escape_name(uint16_t nleng,const uint8_t *name) {
	static char *escname[2]={NULL,NULL};
	static uint32_t escnamesize[2]={0,0};
	static uint8_t buffid=0;
	char *currescname=NULL;
	uint32_t i;
	uint8_t c;
	buffid = 1-buffid;
	i = nleng;
	i = i*3+1;
	if (i>escnamesize[buffid]) {
		escnamesize[buffid] = ((i/1000)+1)*1000;
		if (escname[buffid]!=NULL) {
			free(escname[buffid]);
		}
		escname[buffid] = malloc(escnamesize[buffid]);
	}
	i = 0;
	currescname = escname[buffid];
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
uint32_t fsnodes_hash(uint32_t parentid,uint16_t nleng,const uint8_t *name) {
	uint32_t hash,i;
	hash = ((parentid * 0x5F2318BD) + nleng);
	for (i=0 ; i<nleng ; i++) {
		hash = hash*13+name[i];
	}
	return hash;
}
#endif

int fsnodes_nameisused(fsnode *node,uint16_t nleng,const uint8_t *name) {
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

fsedge* fsnodes_lookup(fsnode *node,uint16_t nleng,const uint8_t *name) {
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


void fsnodes_remove_edge(uint32_t ts,fsedge *e) {
	if (e->parent) {
		e->parent->mtime = e->parent->ctime = ts;
		e->parent->data.ddata.elements--;
		if (e->child->type==TYPE_DIRECTORY) {
			e->parent->data.ddata.nlink--;
		}
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
	free(e->name);
	free(e);
}

void fsnodes_link(uint32_t ts,fsnode *parent,fsnode *child,uint16_t nleng,const uint8_t *name) {
	fsedge *e;
#ifdef EDGEHASH
	uint32_t hpos;
#endif

	e = malloc(sizeof(fsedge));
	e->nleng = nleng;
	e->name = malloc(nleng);
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

	parent->data.ddata.elements++;
	if (child->type==TYPE_DIRECTORY) {
		parent->data.ddata.nlink++;
	}
	if (ts>0) {
		parent->mtime = parent->ctime = ts;
	}
}

fsnode* fsnodes_create_node(uint32_t ts,fsnode* node,uint16_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint32_t uid,uint32_t gid) {
	fsnode *p;
	uint32_t nodepos;
	p = malloc(sizeof(fsnode));
	nodes++;
	if (type==TYPE_DIRECTORY) {
		dirnodes++;
	}
	if (type==TYPE_FILE) {
		filenodes++;
	}
/* link */
//	d->nleng = nleng;
//	d->name = malloc(nleng);
//	memcpy(d->name,name,nleng);
//	d->node = p;
//	d->next = node->data.ddata.children;
//	node->data.ddata.children = d;
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
	p->mode = mode&07777;
	p->uid = uid;
	p->gid = gid;
	switch (type) {
	case TYPE_DIRECTORY:
		p->data.ddata.children = NULL;
		p->data.ddata.nlink = 2;
		p->data.ddata.elements = 0;
		break;
	case TYPE_FILE:
		p->data.fdata.length = 0;
		p->data.fdata.chunks = 0;
		p->data.fdata.chunktab = NULL;
		p->data.fdata.cuids = NULL;
		break;
	case TYPE_SYMLINK:
		p->data.sdata.pleng = 0;
		p->data.sdata.path = NULL;
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		p->data.rdev = 0;
	}
	p->parents = NULL;
//	p->parents = malloc(sizeof(parent));
//	p->parents->node = node;
//	p->parents->next = NULL;
//	node->data.ddata.elements++;
//	if (type==TYPE_DIRECTORY) {
//		node->data.ddata.nlink++;
//	}
//	node->mtime = node->ctime = ts;
	nodepos = NODEHASHPOS(p->id);
	p->next = nodehash[nodepos];
	nodehash[nodepos] = p;
	fsnodes_link(ts,node,p,nleng,name);
	return p;
}

void fsnodes_getpath(fsedge *e,uint16_t *pleng,uint8_t **path) {
	uint32_t size;
	uint8_t *ret;
	fsnode *p;

	p = e->parent;
	size = e->nleng;
	while (p!=root && p->parents) {
		size += p->parents->nleng+1;	// get first parent !!!
		p = p->parents->parent;		// when folders can be hardlinked it's the only way to obtain path (one of them)
	}
	if (size>65535) {
		syslog(LOG_WARNING,"path too long !!! - truncate");
		size=65535;
	}
	*pleng = size;
	ret = malloc(size);
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



void fs_attr_to_attr32(const uint8_t attr[35],uint8_t attr32[32]) {
	uint8_t type;
	type = attr[0];
	attr32[0] = type;
	attr32[1] = 0;	// goal
	memcpy(attr32+2,attr+1,22);	// copy mode,uid,gid,times
	if (type==TYPE_DIRECTORY) {
		memcpy(attr32+24,attr+23,4);
		memcpy(attr32+28,attr+31,4);
	} else if (type==TYPE_SYMLINK) {
		memcpy(attr32+24,attr+31,4);
		attr32[28]=0;
		attr32[29]=0;
		attr32[30]=0;
		attr32[31]=0;
	} else {
		memcpy(attr32+24,attr+27,8);
	}
}

void fs_attr32_to_attrvalues(const uint8_t attr32[32],uint16_t *attrmode,uint32_t *attruid,uint32_t *attrgid,uint32_t *attratime,uint32_t *attrmtime,uint64_t *attrlength) {
	const uint8_t *ptr;
	ptr = attr32+2;
	GET16BIT(*attrmode,ptr);
	ptr = attr32+4;
	GET32BIT(*attruid,ptr);
	ptr = attr32+8;
	GET32BIT(*attrgid,ptr);
	ptr = attr32+12;
	GET32BIT(*attratime,ptr);
	ptr = attr32+16;
	GET32BIT(*attrmtime,ptr);
	ptr = attr32+24;
	GET64BIT(*attrlength,ptr);
}

void fsnodes_fill_attr32(fsnode *node,uint8_t attr[32]) {
	uint8_t *ptr;
	uint8_t t8;
	uint16_t t16;
	uint32_t t32;
	uint64_t t64;
	ptr = attr;
	if (node->type==TYPE_TRASH || node->type==TYPE_RESERVED) {
		PUT8BIT(TYPE_FILE,ptr);
	} else {
		t8 = node->type;
		PUT8BIT(t8,ptr);
	}
	t8 = ((node->goal)&0xF)+((node->trashtime==0)?0x10:0);
	PUT8BIT(t8,ptr);
	t16 = node->mode;
	PUT16BIT(t16,ptr);
	t32 = node->uid;
	PUT32BIT(t32,ptr);
	t32 = node->gid;
	PUT32BIT(t32,ptr);
	t32 = node->atime;
	PUT32BIT(t32,ptr);
	t32 = node->mtime;
	PUT32BIT(t32,ptr);
	t32 = node->ctime;
	PUT32BIT(t32,ptr);
	switch (node->type) {
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		t64 = node->data.fdata.length;
		PUT64BIT(t64,ptr);
		break;
	case TYPE_DIRECTORY:
		t32 = node->data.ddata.nlink;
		PUT32BIT(t32,ptr);
		t32 = node->data.ddata.elements;
		PUT32BIT(t32,ptr);
		break;
	case TYPE_SYMLINK:
		t32 = node->data.sdata.pleng;
		PUT32BIT(t32,ptr);
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		t32 = node->data.rdev;
		PUT32BIT(t32,ptr);
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		break;
	default:
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

void fsnodes_fill_attr(fsnode *node,uint8_t attr[35]) {
	uint8_t *ptr;
	uint8_t t8;
	uint16_t t16;
	uint32_t t32;
	uint64_t t64;
	fsedge *e;
	ptr = attr;
	if (node->type==TYPE_TRASH || node->type==TYPE_RESERVED) {
		PUT8BIT(TYPE_FILE,ptr);
	} else {
		t8 = node->type;
		PUT8BIT(t8,ptr);
	}
	t16 = node->mode;
	PUT16BIT(t16,ptr);
	t32 = node->uid;
	PUT32BIT(t32,ptr);
	t32 = node->gid;
	PUT32BIT(t32,ptr);
	t32 = node->atime;
	PUT32BIT(t32,ptr);
	t32 = node->mtime;
	PUT32BIT(t32,ptr);
	t32 = node->ctime;
	PUT32BIT(t32,ptr);
	t32 = 0;
	for (e=node->parents ; e ; e=e->nextparent) {
		t32++;
	}
	switch (node->type) {
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		PUT32BIT(t32,ptr);
		t64 = node->data.fdata.length;
		PUT64BIT(t64,ptr);
		break;
	case TYPE_DIRECTORY:
		t32 = node->data.ddata.nlink;
		PUT32BIT(t32,ptr);
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		t32 = node->data.ddata.elements;
		PUT32BIT(t32,ptr);
		break;
	case TYPE_SYMLINK:
		PUT32BIT(t32,ptr);
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		t32 = node->data.sdata.pleng;
		PUT32BIT(t32,ptr);
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		PUT32BIT(t32,ptr);
		t32 = node->data.rdev;
		PUT32BIT(t32,ptr);
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		break;
	default:
		PUT32BIT(t32,ptr);
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

uint32_t fsnodes_getdetachedsize(fsedge *start,uint8_t packed) {
	fsedge *e;
	uint32_t result=0;
	if (packed) {
		for (e = start ; e ; e=e->nextchild) {
			if (e->nleng>240) {
				result+=245;
			} else {
				result+=5+e->nleng;
			}
		}
	} else {
		for (e = start ; e ; e=e->nextchild) {
			result+=256+4+32;
		}
	}
	return result;
}

void fsnodes_getdetacheddata(fsedge *start,uint8_t *dbuff,uint8_t packed) {
	fsedge *e;
	uint32_t t32;
	uint8_t *sptr;
	uint8_t c;
	if (packed) {
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
			t32 = e->child->id;
			PUT32BIT(t32,dbuff);
		}
	} else {
		for (e = start ; e ; e=e->nextchild) {
			if (e->nleng>240) {
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
				memset(dbuff,0,16);
				dbuff+=16; // 256-240
			} else {
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
				memset(dbuff,0,256-e->nleng);
				dbuff+=256-e->nleng;
			}
			t32 = e->child->id;
			PUT32BIT(t32,dbuff);
			fsnodes_fill_attr32(e->child,dbuff);
			dbuff+=32;
		}
	}
}


uint32_t fsnodes_getdirsize(fsnode *p,uint8_t packed) {
	if (packed) {
		uint32_t result = 6*2+3;	// for '.' and '..'
		fsedge *e;
		for (e = p->data.ddata.children ; e ; e=e->nextchild) {
			result+=6+e->nleng;
		}
		return result;
	} else {
		return (256+4+1+1+2+4+4+4+4+4+8)*(2+p->data.ddata.elements);	// 292 * (2+elements)
	}
}

void fsnodes_getdirdata(uint32_t ts,fsnode *p,uint8_t *dbuff,uint8_t packed) {
	fsedge *e;
	uint32_t t32;
	uint8_t t8;
	p->atime = ts;
	if (packed) {
		// '.'
//		ptr = p;
		dbuff[0]=1;
		dbuff[1]='.';
		dbuff+=2;
		t32 = p->id;
		PUT32BIT(t32,dbuff);
		t8 = TYPE_DIRECTORY;
		PUT8BIT(t8,dbuff);
		// '..'
//		ptr = p->parent;
		dbuff[0]=2;
		dbuff[1]='.';
		dbuff[2]='.';
		dbuff+=3;
		if (p->parents) {
			t32 = p->parents->parent->id;
		} else {
			t32 = root->id;
		}
		PUT32BIT(t32,dbuff);
		t8 = TYPE_DIRECTORY;
		PUT8BIT(t8,dbuff);
		// entries
		for (e = p->data.ddata.children ; e ; e=e->nextchild) {
			dbuff[0]=e->nleng;
			dbuff++;
			memcpy(dbuff,e->name,e->nleng);
			dbuff+=e->nleng;
			t32 = e->child->id;
			PUT32BIT(t32,dbuff);
			t8 = e->child->type;
			PUT8BIT(t8,dbuff);
		}
	} else {
// .
//		ptr = p;
		dbuff[0]='.';
		memset(dbuff+1,0,255);
		dbuff+=256;
		t32 = p->id;
		PUT32BIT(t32,dbuff);
		fsnodes_fill_attr32(p,dbuff);
		dbuff+=32;
// ..
//		ptr = p->parent;
		dbuff[0]=dbuff[1]='.';
		memset(dbuff+2,0,254);
		dbuff+=256;
		if (p->parents) {
			t32 = p->parents->parent->id;
		} else {
			t32 = root->id;
		}
		PUT32BIT(t32,dbuff);
		if (p->parents) {
			fsnodes_fill_attr32(p->parents->parent,dbuff);
		} else {
			fsnodes_fill_attr32(root,dbuff);
		}
		dbuff+=32;
// entries
		for (e = p->data.ddata.children ; e ; e=e->nextchild) {
			memcpy(dbuff,e->name,e->nleng);
			memset(dbuff+e->nleng,0,256-e->nleng);
			dbuff+=256;
			t32 = e->child->id;
			PUT32BIT(t32,dbuff);
			fsnodes_fill_attr32(e->child,dbuff);
			dbuff+=32;
		}
	}
}

#ifndef METARESTORE
void fsnodes_checkfile(fsnode *p,uint16_t chunkcount[256]) {
	uint32_t i;
	uint64_t chunkid;
	uint8_t count;
	for (i=0 ; i<256 ; i++) {
		chunkcount[i]=0;
	}
	for (i=0 ; i<p->data.fdata.chunks ; i++) {
		chunkid = p->data.fdata.chunktab[i];
		if (chunkid>0) {
			chunk_get_validcopies(chunkid,&count);
			chunkcount[count]++;
		}
	}
}
#endif

uint8_t fsnodes_appendchunks(uint32_t ts,fsnode *dstobj,fsnode *srcobj) {
	uint64_t chunkid,length;
	uint32_t i;
	uint32_t srcchunks,dstchunks;
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
	i = srcchunks+dstchunks-1;	// last new chunk pos
	if (i>MAX_INDEX) {	// chain too long
		return ERROR_INDEXTOOBIG;
	}
	
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
		for (i=dstobj->data.fdata.chunks ; i<newsize ; i++) {
			dstobj->data.fdata.chunktab[i]=0;
		}
		dstobj->data.fdata.chunks = newsize;
	}

	for (i=0 ; i<srcchunks ; i++) {
		chunkid = srcobj->data.fdata.chunktab[i];
		dstobj->data.fdata.chunktab[i+dstchunks] = chunkid;
		if (chunkid>0) {
			if (chunk_add_file(chunkid,dstobj->id,i+dstchunks,dstobj->goal)!=STATUS_OK) {
				syslog(LOG_ERR,"structure error - chunk %llu not found (file: %u ; index: %u)",chunkid,srcobj->id,i);
			}
		}
	}

	length = (((uint64_t)dstchunks)<<26)+srcobj->data.fdata.length;
	if (dstobj->type==TYPE_TRASH) {
		trashspace -= dstobj->data.fdata.length;
		trashspace += length;
	} else if (dstobj->type==TYPE_RESERVED) {
		reservedspace -= dstobj->data.fdata.length;
		reservedspace += length;
	}
	dstobj->data.fdata.length = length;
	dstobj->mtime = ts;
	dstobj->atime = ts;
	srcobj->atime = ts;
	return STATUS_OK;
}

void fsnodes_setchunksgoal(fsnode *obj) {
	uint32_t i;
	for (i=0 ; i<obj->data.fdata.chunks ; i++) {
		if (obj->data.fdata.chunktab[i]>0) {
			chunk_set_file_goal(obj->data.fdata.chunktab[i],obj->id,i,obj->goal);
		}
	}
}

void fsnodes_setlength(fsnode *obj,uint64_t length) {
	uint32_t i,chunks;
	uint64_t chunkid;
	if (obj->type==TYPE_TRASH) {
		trashspace -= obj->data.fdata.length;
		trashspace += length;
	} else if (obj->type==TYPE_RESERVED) {
		reservedspace -= obj->data.fdata.length;
		reservedspace += length;
	}
	obj->data.fdata.length = length;
	if (length>0) {
		chunks = ((length-1)>>26)+1;
	} else {
		chunks = 0;
	}
	for (i=chunks ; i<obj->data.fdata.chunks ; i++) {
		chunkid = obj->data.fdata.chunktab[i];
		if (chunkid>0) {
			if (chunk_delete_file(chunkid,obj->id,i)!=STATUS_OK) {
				syslog(LOG_ERR,"structure error - chunk %llu not found (file: %u ; index: %u)",chunkid,obj->id,i);
			}
		}
		obj->data.fdata.chunktab[i]=0;	// raczej zbedne bo ponizej jest realloc, ale na wszelki wypadek niech bedzie
	}
	if (chunks>0) {
		if (chunks<obj->data.fdata.chunks && obj->data.fdata.chunktab) {
			obj->data.fdata.chunktab = (uint64_t*)realloc(obj->data.fdata.chunktab,sizeof(uint64_t)*chunks);
			obj->data.fdata.chunks = chunks;
		}
	} else {
		if (obj->data.fdata.chunks>0 && obj->data.fdata.chunktab) {
			free(obj->data.fdata.chunktab);
			obj->data.fdata.chunktab = NULL;
			obj->data.fdata.chunks = 0;
		}
	}
}

int fsnodes_isancestor(fsnode *f,fsnode *p) {
	// assert p->type==TYPE_DIRECTORY
	// assert f->type==TYPE_DIRECTORY
	if (f==root) {
		return 1;
		while (p!=root) {
			if (f==p) {
				return 1;
			}
			p = p->parents->parent;
		}
	}
	return 0;
}


void fsnodes_remove_node(uint32_t ts,fsnode *toremove) {
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
// and free
	nodes--;
	if (toremove->type==TYPE_DIRECTORY) {
		dirnodes--;
	}
	if (toremove->type==TYPE_FILE || toremove->type==TYPE_TRASH || toremove->type==TYPE_RESERVED) {
		uint32_t i;
		uint64_t chunkid;
		filenodes--;
		for (i=0 ; i<toremove->data.fdata.chunks ; i++) {
			chunkid = toremove->data.fdata.chunktab[i];
			if (chunkid>0) {
				if (chunk_delete_file(chunkid,toremove->id,i)!=STATUS_OK) {
					syslog(LOG_ERR,"structure error - chunk %llu not found (file: %u ; index: %u)",chunkid,toremove->id,i);
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
	free(toremove);
}


void fsnodes_unlink(uint32_t ts,fsedge *e) {
	fsnode *child;
	uint16_t pleng;
	uint8_t *path;

	child = e->child;
	if (child->parents->nextparent==NULL) { // last link
		if (child->type==TYPE_FILE && (child->trashtime>0 || child->data.fdata.cuids!=NULL)) {	// go to trash or reserved ? - get path
			fsnodes_getpath(e,&pleng,&path);
		}
	}
	fsnodes_remove_edge(ts,e);
	if (child->parents==NULL) {	// last link
		if (child->type == TYPE_FILE) {
			if (child->trashtime>0) {
				child->type = TYPE_TRASH;
				child->ctime = ts;
				e = malloc(sizeof(fsedge));
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
			} else if (child->data.fdata.cuids!=NULL) {
				child->type = TYPE_RESERVED;
				e = malloc(sizeof(fsedge));
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
			} else {
				fsnodes_remove_node(ts,child);
			}
		} else {
			fsnodes_remove_node(ts,child);
		}
	}
}

int fsnodes_purge(uint32_t ts,fsnode *p) {
	fsedge *e;
	e = p->parents;

	if (p->type==TYPE_TRASH) {
		trashspace -= p->data.fdata.length;
		trashnodes--;
		if (p->data.fdata.cuids!=NULL) {
			p->type = TYPE_RESERVED;
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

uint8_t fsnodes_undel(uint32_t ts,fsnode *node) {
	uint16_t pleng;
	const uint8_t *path;
	uint8_t new;
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
		if (path[i]==0) {	// incorrect name character
			return ERROR_CANTCREATEPATH;
		} else if (path[i]=='/') {
			if (partleng==0) {	// "//" in path
				return ERROR_CANTCREATEPATH;
			}
			if (partleng==dots && partleng<=2) {	// '.' or '..' in path
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
	if (partleng==0) {	// last part canot be empty - it's the name of undeleted file
		return ERROR_CANTCREATEPATH;
	}
	if (partleng==dots && partleng<=2) {	// '.' or '..' in path
		return ERROR_CANTCREATEPATH;
	}

/* create path */
	n = NULL;
	p = root;
	new = 0;
	for (;;) {
		partleng=0;
		while (path[partleng]!='/' && partleng<pleng) {
			partleng++;
		}
		if (partleng==pleng) {	// last name
			if (fsnodes_nameisused(p,partleng,path)) {
				return ERROR_EEXIST;
			}
			// remove from trash and link to new parent
			fsnodes_link(ts,p,node,partleng,path);
			fsnodes_remove_edge(ts,e);
			node->type = TYPE_FILE;
			node->ctime = ts;
			trashspace -= node->data.fdata.length;
			trashnodes--;
			return STATUS_OK;
		} else {
			if (new==0) {
				pe = fsnodes_lookup(p,partleng,path);
				if (pe==NULL) {
					new=1;
				} else {
					n = pe->child;
					if (n->type!=TYPE_DIRECTORY) {
						return ERROR_CANTCREATEPATH;
					}
				}
			}
			if (new==1) {
				n = fsnodes_create_node(ts,p,partleng,path,TYPE_DIRECTORY,0755,0,0);
			}
			p = n;
		}
		path+=partleng+1;
		pleng-=partleng+1;
	}
}


#ifndef METARESTORE
void fsnodes_get_file_stats(fsnode *node,uint32_t *undergoalfiles,uint32_t *missingfiles,uint32_t *chunks,uint32_t *undergoalchunks,uint32_t *missingchunks,uint64_t *length,uint64_t *size,uint64_t *gsize) {
	uint32_t i,ug,m,lastchunk,lastchunksize;
	uint8_t cnt;
	//assert(node->type==TYPE_FILE);
	(*length)+=node->data.fdata.length;
	if (node->data.fdata.length>0) {
		lastchunk = (node->data.fdata.length-1)>>26;
		lastchunksize = ((((node->data.fdata.length-1)&0x3FFFFFF)+0x10000)&0x7FFF0000)+0x1400;
	} else {
		lastchunk = 0;
		lastchunksize = 0x1400;
	}
	ug=0;
	m=0;
	for (i=0 ; i<node->data.fdata.chunks ; i++) {
		if (node->data.fdata.chunktab[i]>0) {
			chunk_get_validcopies(node->data.fdata.chunktab[i],&cnt);
			if (cnt<node->goal) {
				if (cnt==0) {
					m=1;
					(*missingchunks)++;
				} else {
					ug=1;
					//(*undergoalchunks)+=((node->goal)-cnt);
					(*undergoalchunks)++;
				}
			}
			if (i<lastchunk) {
				(*size)+=0x4001400UL;
				(*gsize)+=cnt*0x4001400UL;
			} else if (i==lastchunk) {
				(*size)+=lastchunksize;
				(*gsize)+=cnt*lastchunksize;
			}
			//(*chunks)+=cnt;
			(*chunks)++;
		}
	}
	(*undergoalfiles) += ug;
	(*missingfiles) += m;
}

void fsnodes_get_dir_stats(fsnode *node,uint32_t *inodes,uint32_t *dirs,uint32_t *files,uint32_t *undergoalfiles,uint32_t *missingfiles,uint32_t *chunks,uint32_t *undergoalchunks,uint32_t *missingchunks,uint64_t *length,uint64_t *size,uint64_t *gsize) {
	uint32_t i,ug,m,lastchunk,lastchunksize;
	uint8_t cnt;
	fsedge *e;
	fsnode *n;
	//assert(node->type==TYPE_DIRECTORY);
	for (e = node->data.ddata.children ; e ; e=e->nextchild) {
		n=e->child;
		(*inodes)++;
		if (n->type==TYPE_FILE) {
			(*length)+=n->data.fdata.length;
			if (n->data.fdata.length>0) {
				lastchunk = (n->data.fdata.length-1)>>26;
				lastchunksize = ((((n->data.fdata.length-1)&0x3FFFFFF)+0x10000)&0x7FFF0000)+0x1400;
			} else {
				lastchunk = 0;
				lastchunksize = 0x1400;
			}
			ug=0;
			m=0;
			for (i=0 ; i<n->data.fdata.chunks ; i++) {
				if (n->data.fdata.chunktab[i]>0) {
					chunk_get_validcopies(n->data.fdata.chunktab[i],&cnt);
					if (cnt<n->goal) {
						if (cnt==0) {
							m=1;
							(*missingchunks)++;
						} else {
							ug=1;
							//(*undergoalchunks)+=((ptr->goal)-cnt);
							(*undergoalchunks)++;
						}
					}
					if (i<lastchunk) {
						(*size)+=0x4001400UL;
						(*gsize)+=cnt*0x4001400UL;
					} else if (i==lastchunk) {
						(*size)+=lastchunksize;
						(*gsize)+=cnt*lastchunksize;
					}
					//(*chunks)+=cnt;
					(*chunks)++;
				}
			}
			(*undergoalfiles) += ug;
			(*missingfiles) += m;
			(*files)++;
		} else if (n->type==TYPE_DIRECTORY) {
			fsnodes_get_dir_stats(n,inodes,dirs,files,undergoalfiles,missingfiles,chunks,undergoalchunks,missingchunks,length,size,gsize);
			(*dirs)++;
		}
	}
}

void fsnodes_getgoal_recursive(fsnode *node,uint32_t fgtab[10],uint32_t dgtab[10]) {
	fsedge *e;
	fsnode *n;
	// assert (node->type==TYPE_DIRECTORY);
	if (node->goal>9) {
		syslog(LOG_WARNING,"inode %u: goal>9 !!! - fixing",node->id);
		node->goal=9;
	} else if (node->goal<1) {
		syslog(LOG_WARNING,"inode %u: goal<1 !!! - fixing",node->id);
		node->goal=1;
	}
	dgtab[node->goal]++;
	for (e = node->data.ddata.children ; e ; e=e->nextchild) {
		n = e->child;
		if (n->type==TYPE_FILE) {
			if (n->goal>9) {
				syslog(LOG_WARNING,"inode %u: goal>9 !!! - fixing",n->id);
				n->goal=9;
				fsnodes_setchunksgoal(n);
			} else if (n->goal<1) {
				syslog(LOG_WARNING,"inode %u: goal<1 !!! - fixing",n->id);
				n->goal=1;
				fsnodes_setchunksgoal(n);
			}
			fgtab[n->goal]++;
		} else if (n->type==TYPE_DIRECTORY) {
			fsnodes_getgoal_recursive(n,fgtab,dgtab);
		}
	}
}

/*
void fsnodes_getgoal_recursive(fsnode *node,uint32_t gtab[10]) {
	fsedge *e;
	fsnode *n;
	for (e = node->data.ddata.children ; e ; e=e->nextchild) {
		n = e->child;
		if (n->type==TYPE_FILE) {
			if (n->goal>9) {
				syslog(LOG_WARNING,"inode %u: goal>9 !!! - fixing",n->id);
				n->goal=9;
				fsnodes_setchunksgoal(n);
			} else if (n->goal<1) {
				syslog(LOG_WARNING,"inode %u: goal<1 !!! - fixing",n->id);
				n->goal=1;
				fsnodes_setchunksgoal(n);
			}
			gtab[n->goal]++;
		} else if (n->type==TYPE_DIRECTORY) {
			fsnodes_getgoal_recursive(n,gtab);
		}
	}
}
*/

void fsnodes_bst_add(bstnode **n,uint32_t val) {
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
	(*n)=malloc(sizeof(bstnode));
	(*n)->val = val;
	(*n)->count = 1;
	(*n)->left = NULL;
	(*n)->right = NULL;
}

uint32_t fsnodes_bst_nodes(bstnode *n) {
	if (n) {
		return 1+fsnodes_bst_nodes(n->left)+fsnodes_bst_nodes(n->right);
	} else {
		return 0;
	}
}

void fsnodes_bst_storedata(bstnode *n,uint8_t **ptr) {
	if (n) {
		fsnodes_bst_storedata(n->left,ptr);
		PUT32BIT(n->val,*ptr);
		PUT32BIT(n->count,*ptr);
		fsnodes_bst_storedata(n->right,ptr);
	}
}

void fsnodes_bst_free(bstnode *n) {
	if (n) {
		fsnodes_bst_free(n->left);
		fsnodes_bst_free(n->right);
		free(n);
	}
}

/*
void fsnodes_gettrashtime_recursive(fsnode *node,bstnode **bstroot) {
	fsedge *e;
	fsnode *n;
	for (e = node->data.ddata.children ; e ; e=e->nextchild) {
		n = e->child;
		if (n->type==TYPE_FILE) {
			fsnodes_bst_add(bstroot,n->trashtime);
		} else if (n->type==TYPE_DIRECTORY) {
			fsnodes_gettrashtime_recursive(n,bstroot);
		}
	}
}
*/

void fsnodes_gettrashtime_recursive(fsnode *node,bstnode **bstrootfiles,bstnode **bstrootdirs) {
	fsedge *e;
	fsnode *n;
	// assert (node->type==TYPE_DIRECTORY);
	fsnodes_bst_add(bstrootdirs,node->trashtime);
	for (e = node->data.ddata.children ; e ; e=e->nextchild) {
		n = e->child;
		if (n->type==TYPE_FILE) {
			fsnodes_bst_add(bstrootfiles,n->trashtime);
		} else if (n->type==TYPE_DIRECTORY) {
			fsnodes_gettrashtime_recursive(n,bstrootfiles,bstrootdirs);
		}
	}
}


#endif

void fsnodes_setgoal_recursive(fsnode *node,uint32_t ts,uint32_t uid,uint8_t goal,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	fsedge *e;
	fsnode *n;
	uint8_t set;

	if (uid!=0 && node->uid!=uid) {
		(*nsinodes)++;
	} else {
		set=0;
		switch (smode&SMODE_TMASK) {
		case SMODE_SET:
			if (node->goal!=goal) {
				node->goal=goal;
				set=1;
			}
			break;
		case SMODE_INCREASE:
			if (node->goal<goal) {
				node->goal=goal;
				set=1;
			}
			break;
		case SMODE_DECREASE:
			if (node->goal>goal) {
				node->goal=goal;
				set=1;
			}
			break;
		}
		if (set) {
			(*sinodes)++;
		} else {
			(*ncinodes)++;
		}
		node->ctime = ts;
	}
	for (e = node->data.ddata.children ; e ; e=e->nextchild) {
		n = e->child;
		if (n->type==TYPE_FILE) {
			if (uid!=0 && n->uid!=uid) {
				(*nsinodes)++;
			} else {
				set=0;
				switch (smode) {
				case SMODE_SET:
					if (n->goal!=goal) {
						n->goal=goal;
						set=1;
					}
					break;
				case SMODE_INCREASE:
					if (n->goal<goal) {
						n->goal=goal;
						set=1;
					}
					break;
				case SMODE_DECREASE:
					if (n->goal>goal) {
						n->goal=goal;
						set=1;
					}
					break;
				}
				if (set) {
					(*sinodes)++;
					fsnodes_setchunksgoal(n);
				} else {
					(*ncinodes)++;
				}
				n->ctime = ts;
			}
		} else if (n->type==TYPE_DIRECTORY) {
			fsnodes_setgoal_recursive(n,ts,uid,goal,smode,sinodes,ncinodes,nsinodes);
		}
	}
}


void fsnodes_settrashtime_recursive(fsnode *node,uint32_t ts,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	fsedge *e;
	fsnode *n;
	uint8_t set;

	if (uid!=0 && node->uid!=uid) {
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
		} else {
			(*ncinodes)++;
		}
		node->ctime = ts;
	}
	for (e = node->data.ddata.children ; e ; e=e->nextchild) {
		n = e->child;
		if (n->type==TYPE_FILE) {
			if (uid!=0 && n->uid!=uid) {
				(*nsinodes)++;
			} else {
				set=0;
				switch (smode) {
				case SMODE_SET:
					if (n->trashtime!=trashtime) {
						n->trashtime=trashtime;
						set=1;
					}
					break;
				case SMODE_INCREASE:
					if (n->trashtime<trashtime) {
						n->trashtime=trashtime;
						set=1;
					}
					break;
				case SMODE_DECREASE:
					if (n->trashtime>trashtime) {
						n->trashtime=trashtime;
						set=1;
					}
					break;
				}
				if (set) {
					(*sinodes)++;
				} else {
					(*ncinodes)++;
				}
				n->ctime = ts;
			}
		} else if (n->type==TYPE_DIRECTORY) {
			fsnodes_settrashtime_recursive(n,ts,uid,trashtime,smode,sinodes,ncinodes,nsinodes);
		}
	}
}

int fsnodes_namecheck(uint32_t nleng,const uint8_t *name) {
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


fsnode* fsnodes_id_to_node(uint32_t id) {
	fsnode *p;
	uint32_t nodepos = NODEHASHPOS(id);
	for (p=nodehash[nodepos]; p ; p=p->next ) {
		if (p->id == id) {
			return p;
		}
	}
	return NULL;
}

int fsnodes_access(fsnode *node,uint32_t uid,uint32_t gid,int modemask) {
	int nodemode;
	if (uid==0) {
		return 1;
	}
	if (uid==node->uid) {
		nodemode = ((node->mode)>>6) & 7;
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


/* master <-> fuse operations */

#ifdef METARESTORE
uint8_t fs_access(uint32_t ts,uint32_t inode) {
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	p->atime = ts;
	version++;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint32_t fs_readreserved_size(uint8_t packed) {
	return fsnodes_getdetachedsize(reserved,packed);
}

void fs_readreserved_data(uint8_t *dbuff,uint8_t packed) {
	fsnodes_getdetacheddata(reserved,dbuff,packed);
}


uint32_t fs_readtrash_size(uint8_t packed) {
	return fsnodes_getdetachedsize(trash,packed);
}

void fs_readtrash_data(uint8_t *dbuff,uint8_t packed) {
	fsnodes_getdetacheddata(trash,dbuff,packed);
}

/* common procedure for trash and reserved files */
uint8_t fs_getdetachedattr(uint32_t inode,uint8_t attr[35],uint8_t dtype) {
	fsnode *p;
	memset(attr,0,35);
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
	fsnodes_fill_attr(p,attr);
	return STATUS_OK;
}

uint8_t fs_gettrashpath(uint32_t inode,uint32_t *pleng,uint8_t **path) {
	fsnode *p;
	*pleng = 0;
	*path = NULL;
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
uint8_t fs_settrashpath(uint32_t inode,uint32_t pleng,uint8_t *path) {
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
	newpath = malloc(pleng);
	if (newpath==NULL) {
		return ERROR_EINVAL;	// no mem ?
	}
	free(p->parents->name);
	memcpy(newpath,path,pleng);
	p->parents->name = newpath;
	p->parents->nleng = pleng;
#ifndef METARESTORE
	changelog(version++,"%u|SETPATH(%u,%s)",main_time(),inode,fsnodes_escape_name(pleng,newpath));
#else
	version++;
#endif
	return STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_undel(uint32_t inode) {
	uint32_t ts;
#else
uint8_t fs_undel(uint32_t ts,uint32_t inode) {
#endif
	fsnode *p;
	uint8_t status;
#ifndef METARESTORE
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
		changelog(version++,"%u|UNDEL(%u)",ts,inode);
	}
#else
	version++;
#endif
	return status;
}

#ifndef METARESTORE
uint8_t fs_purge(uint32_t inode) {
	uint32_t ts;
#else
uint8_t fs_purge(uint32_t ts,uint32_t inode) {
#endif
	fsnode *p;
#ifndef METARESTORE
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
	changelog(version++,"%u|PURGE(%u)",ts,inode);
#else
	version++;
#endif
	return STATUS_OK;
}

#ifndef METARESTORE
void fs_info(uint64_t *totalspace,uint64_t *availspace,uint64_t *trspace,uint32_t *trnodes,uint64_t *respace,uint32_t *renodes,uint32_t *inodes,uint32_t *dnodes,uint32_t *fnodes,uint32_t *chunks,uint32_t *tdchunks) {
	matocsserv_getspace(totalspace,availspace);
	*trspace = trashspace;
	*trnodes = trashnodes;
	*respace = reservedspace;
	*renodes = reservednodes;
	*inodes = nodes;
	*dnodes = dirnodes;
	*fnodes = filenodes;
	*chunks = chunk_count();
	*tdchunks = chunk_todel_count();
}

void fs_statfs(uint64_t *totalspace,uint64_t *availspace,uint64_t *trspace,uint64_t *respace,uint32_t *inodes) {
	matocsserv_getspace(totalspace,availspace);
	*inodes = nodes;
	*trspace = trashspace;
	*respace = reservedspace;
	stats_statfs++;
}

uint8_t fs_access(uint32_t inode,uint32_t uid,uint32_t gid,int modemask) {
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	return fsnodes_access(p,uid,gid,modemask)?STATUS_OK:ERROR_EACCES;
}

uint8_t fs_lookup(uint32_t parent,uint16_t nleng,uint8_t *name,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]) {
	fsnode *wd;
	fsedge *e;
	*inode = 0;
	memset(attr,0,35);
	wd = fsnodes_id_to_node(parent);
	if (!wd) {
		return ERROR_ENOENT;
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if (!fsnodes_access(wd,uid,gid,MODE_MASK_X)) {
		return ERROR_EACCES;
	}
	if (name[0]=='.') {
		if (nleng==1) {	// self
			*inode=wd->id;
			fsnodes_fill_attr(wd,attr);
			stats_lookup++;
			return STATUS_OK;
		}
		if (nleng==2 && name[1]=='.') {	// parent
			if (wd->parents) {
				*inode=wd->parents->parent->id;
				fsnodes_fill_attr(wd->parents->parent,attr);
			} else {
				*inode=root->id;
				fsnodes_fill_attr(root,attr);
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
	fsnodes_fill_attr(e->child,attr);
	stats_lookup++;
	return STATUS_OK;
}

uint8_t fs_getattr(uint32_t inode,uint8_t attr[35]) {
	fsnode *p;
	memset(attr,0,35);
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	fsnodes_fill_attr(p,attr);
	stats_getattr++;
	return STATUS_OK;
}

#if 0
int fs_try_setattr(uint32_t inode,uint32_t uid,uint32_t gid,uint16_t setmask,uint8_t attr[35],uint64_t *chunkid) {
	uint32_t attruid,attrgid;
	uint8_t *ptr;
	fsnode *p;
	if (setmask==0) {
		memset(attr,0,35);
		return ERROR_EINVAL;
	}
	if ((setmask&SET_GOAL_FLAG) && ((attr[1]&0x0F)==0 || (attr[1]&0x0F)>9)) {
		memset(attr,0,35);
		return ERROR_EINVAL;
	}
	p = fsnodes_id_to_node(inode);
	if (!p) {
		memset(attr,0,35);
		return ERROR_ENOENT;
	}
	ptr = attr+4;
	GET32BIT(attruid,ptr);
	ptr = attr+8;
	GET32BIT(attrgid,ptr);
	if (uid!=0 && uid!=p->uid && (setmask&(SET_GOAL_FLAG|SET_DELETE_FLAG|SET_MODE_FLAG|SET_UID_FLAG|SET_GID_FLAG|SET_ATIME_FLAG|SET_MTIME_FLAG))) {
		memset(attr,0,35);
		return ERROR_EPERM;
	}
	if (uid!=0 && uid!=attruid && (setmask&SET_UID_FLAG)) {
		memset(attr,0,35);
		return ERROR_EPERM;
	}
	if (uid!=0 && gid!=attrgid && (setmask&SET_GID_FLAG)) {
		memset(attr,0,35);
		return ERROR_EPERM;
	}
	if ((setmask&SET_LENGTH_FLAG) && ((setmask&SET_OPENED_FLAG)==0) && !fsnodes_access(p,uid,gid,MODE_MASK_W)) {
		memset(attr,0,35);
		return ERROR_EACCES;
	}
	if ((setmask&(SET_GOAL_FLAG|SET_DELETE_FLAG)) && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED && p->type!=TYPE_DIRECTORY) {
		memset(attr,0,35);
		return ERROR_EPERM;
	}
	if ((setmask&SET_LENGTH_FLAG) && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		memset(attr,0,35);
		return ERROR_EPERM;
	}

	if (setmask&SET_LENGTH_FLAG) {
		uint64_t attrlength;
		ptr = attr+24;
		GET64BIT(attrlength,ptr);
		if (attrlength&0x3FFFFFF) {
			uint32_t indx = (attrlength>>26);
			if (indx<p->data.fdata.chunks) {
				uint64_t ochunkid = p->data.fdata.chunktab[indx];
				if (ochunkid>0) {
					uint8_t status;
					uint64_t nchunkid;
					status = chunk_multi_truncate(&nchunkid,ochunkid,attrlength&0x3FFFFFF,inode,indx,p->goal);
/*
					status = chunk_locked(ochunkid,&l);
					if (status!=STATUS_OK) {
						syslog(LOG_ERR,"structure error - chunk %llu not found (file: %u ; index: %u)",ochunkid,inode,indx);
						return status;
					}
					if (l) {
						return ERROR_LOCKED;
					}
					chunk_get_refcount(ochunkid,&refcount);	// can ignore status (ERROR_NOCHUNK - impossible after previous functions)
					if (refcount>1) {
						status = chunk_duptrunc(&nchunkid,ochunkid,attrlength&0x3FFFFFF,p->goal);
						if (status!=STATUS_OK) {
							return status;
						}
						chunk_decrease_refcount(ochunkid);	// can ignore status (ERROR_NOCHUNK - impossible after previous functions)
						p->data.fdata.chunktab[indx] = nchunkid;
					} else {
						nchunkid = ochunkid;
						status = chunk_truncate(nchunkid,attrlength&0x3FFFFFF);
						if (status!=STATUS_OK) {
							return status;
						}
					}
*/
					if (status!=STATUS_OK) {
						return status;
					}
					p->data.fdata.chunktab[indx] = nchunkid;
					*chunkid = nchunkid;
//					chunk_writelock(nchunkid); // can ignore status (ERROR_NOCHUNK - impossible after previous functions)
					changelog(version++,"%u|TRUNC(%u,%u):%llu",main_time(),inode,indx,nchunkid);
					return ERROR_DELAYED;
				}
			}
		}
	}
	stats_setattr++;
	return STATUS_OK;
}
#endif

uint8_t fs_try_setlength(uint32_t inode,uint32_t uid,uint32_t gid,uint64_t length,uint8_t attr[35],uint64_t *chunkid) {
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		memset(attr,0,35);
		return ERROR_ENOENT;
	}
	if (!fsnodes_access(p,uid,gid,MODE_MASK_W)) {
		memset(attr,0,35);
		return ERROR_EACCES;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		memset(attr,0,35);
		return ERROR_EPERM;
	}

	if (length&0x3FFFFFF) {
		uint32_t indx = (length>>26);
		if (indx<p->data.fdata.chunks) {
			uint64_t ochunkid = p->data.fdata.chunktab[indx];
			if (ochunkid>0) {
				uint8_t status;
				uint64_t nchunkid;
				status = chunk_multi_truncate(&nchunkid,ochunkid,length&0x3FFFFFF,inode,indx,p->goal);
				if (status!=STATUS_OK) {
					return status;
				}
				p->data.fdata.chunktab[indx] = nchunkid;
				*chunkid = nchunkid;
				changelog(version++,"%u|TRUNC(%u,%u):%llu",main_time(),inode,indx,nchunkid);
				return ERROR_DELAYED;
			}
		}
	}
	fsnodes_fill_attr(p,attr);
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
	status = chunk_multi_truncate(ts,&nchunkid,ochunkid,inode,indx,p->goal);
	if (status!=STATUS_OK) {
		return status;
	}
	if (chunkid!=nchunkid) {
		return ERROR_MISMATCH;
	}
	p->data.fdata.chunktab[indx] = nchunkid;
	version++;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint8_t fs_end_setlength(uint64_t chunkid) {
	changelog(version++,"%u|UNLOCK(%llu)",main_time(),chunkid);
	return chunk_unlock(chunkid);
}
#else
uint8_t fs_unlock(uint64_t chunkid) {
	version++;
	return chunk_unlock(chunkid);
}
#endif

#ifndef METARESTORE
uint8_t fs_do_setlength(uint32_t inode,uint64_t length,uint8_t attr[35]) {
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		memset(attr,0,35);
		return ERROR_ENOENT;
	}
	fsnodes_setlength(p,length);
	changelog(version++,"%u|LENGTH(%u,%llu)",main_time(),inode,p->data.fdata.length);
	p->ctime = p->mtime = main_time();
	fsnodes_fill_attr(p,attr);
	stats_setattr++;
	return STATUS_OK;
}

uint8_t fs_setattr(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t setmask,uint16_t attrmode,uint32_t attruid,uint32_t attrgid,uint32_t attratime,uint32_t attrmtime,uint8_t attr[35]) {
	fsnode *p;
	if (setmask==0) {
		memset(attr,0,35);
		return ERROR_EINVAL;
	}
	p = fsnodes_id_to_node(inode);
	if (!p) {
		memset(attr,0,35);
		return ERROR_ENOENT;
	}
	if ((setmask&(SET_MODE_FLAG|SET_UID_FLAG|SET_GID_FLAG|SET_ATIME_FLAG|SET_MTIME_FLAG))==0) {
		fsnodes_fill_attr(p,attr);
		stats_setattr++;
		return STATUS_OK;
	}
	if (uid!=0 && uid!=p->uid && (setmask&(SET_MODE_FLAG|SET_UID_FLAG|SET_GID_FLAG|SET_ATIME_FLAG|SET_MTIME_FLAG))) {
		memset(attr,0,35);
		return ERROR_EPERM;
	}
	if (uid!=0 && uid!=attruid && (setmask&SET_UID_FLAG)) {
		memset(attr,0,35);
		return ERROR_EPERM;
	}
	if (uid!=0 && gid!=attrgid && (setmask&SET_GID_FLAG)) {
		memset(attr,0,35);
		return ERROR_EPERM;
	}
	if (setmask&SET_MODE_FLAG) {
		p->mode = attrmode & 07777;
	}
	if (setmask&SET_UID_FLAG) {
		p->uid = attruid;
	}
	if (setmask&SET_GID_FLAG) {
		p->gid = attrgid;
	}
	if (setmask&SET_ATIME_FLAG) {
		p->atime = attratime;
	}
	if (setmask&SET_MTIME_FLAG) {
		p->mtime = attrmtime;
	}
	changelog(version++,"%u|ATTR(%u,%u,%u,%u,%u,%u)",main_time(),inode,p->mode,p->uid,p->gid,p->atime,p->mtime);
	p->ctime = main_time();
	fsnodes_fill_attr(p,attr);
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
	p->mode = mode;
	p->uid = uid;
	p->gid = gid;
	p->atime = atime;
	p->mtime = mtime;
	p->ctime = ts;
	version++;
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
	version++;
	return STATUS_OK;
}

#endif

/*
#ifndef METARESTORE
uint8_t fs_gettrash_timeout(uint32_t inode,uint32_t *trashto) {
	fsnode *p;
	*trashto = 0;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	*trashto = p->trashtime;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint8_t fs_settrash_timeout(uint32_t inode,uint32_t uid,uint32_t trashto) {
	uint32_t ts;
#else
uint8_t fs_settrashtime(uint32_t ts,uint32_t inode,uint32_t trashto) {
#endif
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
#ifndef METARESTORE
	if (uid!=0 && uid!=p->uid) {
		return ERROR_EPERM;
	}
	ts = main_time();
#endif
	p->trashtime = trashto;
	p->ctime = ts;
#ifndef METARESTORE
	changelog(version++,"%u|SETTRASHTIME(%u,%u)",ts,inode,p->trashtime);
#else
	version++;
#endif
	return STATUS_OK;
}
*/

#ifndef METARESTORE
uint8_t fs_readlink(uint32_t inode,uint32_t *pleng,uint8_t **path) {
	fsnode *p;
	*pleng = 0;
	*path = NULL;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_SYMLINK) {
		return ERROR_EINVAL;
	}
	*pleng = p->data.sdata.pleng;
	*path = p->data.sdata.path;
	p->atime = main_time();
	changelog(version++,"%u|ACCESS(%u)",main_time(),inode);
	stats_readlink++;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint8_t fs_symlink(uint32_t parent,uint16_t nleng,uint8_t *name,uint32_t pleng,uint8_t *path,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]) {
#else
uint8_t fs_symlink(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,const uint8_t *path,uint32_t uid,uint32_t gid,uint32_t inode) {
	uint32_t pleng;
#endif
	fsnode *wd,*p;
	uint8_t *newpath;
#ifndef METARESTORE
	uint32_t i;
	*inode = 0;
	memset(attr,0,35);
	if (pleng==0) {
		return ERROR_EINVAL;
	}
	for (i=0 ; i<pleng ; i++) {
		if (path[i]==0) {
			return ERROR_EINVAL;
		}
	}
#else
	pleng = strlen((const char*)path);
#endif
	wd = fsnodes_id_to_node(parent);
	if (!wd) {
		return ERROR_ENOENT;
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
#ifndef METARESTORE
	if (!fsnodes_access(wd,uid,gid,MODE_MASK_W)) {
		return ERROR_EACCES;
	}
#endif
	if (fsnodes_namecheck(nleng,name)<0) {
		return ERROR_EINVAL;
	}
	if (fsnodes_nameisused(wd,nleng,name)) {
		return ERROR_EEXIST;
	}
	newpath = malloc(pleng);
	if (newpath==NULL) {
		return ERROR_EINVAL;	// no mem ?
	}
#ifndef METARESTORE
	p = fsnodes_create_node(main_time(),wd,nleng,name,TYPE_SYMLINK,0777,uid,gid);
#else
	p = fsnodes_create_node(ts,wd,nleng,name,TYPE_SYMLINK,0777,uid,gid);
#endif
	memcpy(newpath,path,pleng);
	p->data.sdata.path = newpath;
	p->data.sdata.pleng = pleng;
#ifndef METARESTORE
	*inode = p->id;
	fsnodes_fill_attr(p,attr);
	changelog(version++,"%u|SYMLINK(%u,%s,%s,%u,%u):%u",main_time(),parent,fsnodes_escape_name(nleng,name),fsnodes_escape_name(pleng,newpath),uid,gid,p->id);
	stats_symlink++;
#else
	if (inode!=p->id) {
		return ERROR_MISMATCH;
	}
	version++;
#endif
	return STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_mknod(uint32_t parent,uint16_t nleng,uint8_t *name,uint8_t type,uint16_t mode,uint32_t uid,uint32_t gid,uint32_t rdev,uint32_t *inode,uint8_t attr[35]) {
	fsnode *wd,*p;
	*inode = 0;
	memset(attr,0,35);
	if (type!=TYPE_FILE && type!=TYPE_SOCKET && type!=TYPE_FIFO && type!=TYPE_BLOCKDEV && type!=TYPE_CHARDEV) {
		return ERROR_EINVAL;
	}
	wd = fsnodes_id_to_node(parent);
	if (!wd) {
		return ERROR_ENOENT;
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if (!fsnodes_access(wd,uid,gid,MODE_MASK_W)) {
		return ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return ERROR_EINVAL;
	}
	if (fsnodes_nameisused(wd,nleng,name)) {
		return ERROR_EEXIST;
	}
	p = fsnodes_create_node(main_time(),wd,nleng,name,type,mode,uid,gid);
	if (type==TYPE_BLOCKDEV || type==TYPE_CHARDEV) {
		p->data.rdev = rdev;
	}
	*inode = p->id;
	fsnodes_fill_attr(p,attr);
	changelog(version++,"%u|CREATE(%u,%s,%c,%u,%u,%u,%u):%u",main_time(),parent,fsnodes_escape_name(nleng,name),type,mode,uid,gid,rdev,p->id);
	stats_mknod++;
	return STATUS_OK;
}

uint8_t fs_mkdir(uint32_t parent,uint16_t nleng,uint8_t *name,uint16_t mode,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]) {
	fsnode *wd,*p;
	*inode = 0;
	memset(attr,0,35);
	wd = fsnodes_id_to_node(parent);
	if (!wd) {
		return ERROR_ENOENT;
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if (!fsnodes_access(wd,uid,gid,MODE_MASK_W)) {
		return ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return ERROR_EINVAL;
	}
	if (fsnodes_nameisused(wd,nleng,name)) {
		return ERROR_EEXIST;
	}
	p = fsnodes_create_node(main_time(),wd,nleng,name,TYPE_DIRECTORY,mode,uid,gid);
	*inode = p->id;
	fsnodes_fill_attr(p,attr);
	changelog(version++,"%u|CREATE(%u,%s,%c,%u,%u,%u,%u):%u",main_time(),parent,fsnodes_escape_name(nleng,name),TYPE_DIRECTORY,mode,uid,gid,0,p->id);
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
	p = fsnodes_create_node(ts,wd,nleng,name,type,mode,uid,gid);
	if (type==TYPE_BLOCKDEV || type==TYPE_CHARDEV) {
		p->data.rdev = rdev;
	}
	if (inode!=p->id) {
		return ERROR_MISMATCH;
	}
	version++;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint8_t fs_unlink(uint32_t parent,uint16_t nleng,uint8_t *name,uint32_t uid,uint32_t gid) {
	uint32_t ts;
	fsnode *wd;
	fsedge *e;
	ts = main_time();
	wd = fsnodes_id_to_node(parent);
	if (!wd) {
		return ERROR_ENOENT;
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if (!fsnodes_access(wd,uid,gid,MODE_MASK_W)) {
		return ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return ERROR_EINVAL;
	}
	e = fsnodes_lookup(wd,nleng,name);
	if (!e) {
		return ERROR_ENOENT;
	}
	if (e->child->type==TYPE_DIRECTORY) {
		return ERROR_EPERM;
	}
	changelog(version++,"%u|UNLINK(%u,%s):%u",ts,parent,fsnodes_escape_name(nleng,name),e->child->id);
	fsnodes_unlink(ts,e);
	stats_unlink++;
	return STATUS_OK;

}

uint8_t fs_rmdir(uint32_t parent,uint16_t nleng,uint8_t *name,uint32_t uid,uint32_t gid) {
	uint32_t ts;
	fsnode *wd;
	fsedge *e;
	ts = main_time();
	wd = fsnodes_id_to_node(parent);
	if (!wd) {
		return ERROR_ENOENT;
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if (!fsnodes_access(wd,uid,gid,MODE_MASK_W)) {
		return ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return ERROR_EINVAL;
	}
	e = fsnodes_lookup(wd,nleng,name);
	if (!e) {
		return ERROR_ENOENT;
	}
	if (e->child->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if (e->child->data.ddata.children!=NULL) {
		return ERROR_ENOTEMPTY;
	}
	changelog(version++,"%u|UNLINK(%u,%s):%u",ts,parent,fsnodes_escape_name(nleng,name),e->child->id);
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
	version++;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint8_t fs_rename(uint32_t parent_src,uint16_t nleng_src,uint8_t *name_src,uint32_t parent_dst,uint16_t nleng_dst,uint8_t *name_dst,uint32_t uid,uint32_t gid) {
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
	ts = main_time();
#endif
	swd = fsnodes_id_to_node(parent_src);
	if (!swd) {
		return ERROR_ENOENT;
	}
	if (swd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
#ifndef METARESTORE
	if (!fsnodes_access(swd,uid,gid,MODE_MASK_W)) {
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
#ifdef METARESTORE
	if (node->id!=inode) {
		return ERROR_MISMATCH;
	}
#endif
	dwd = fsnodes_id_to_node(parent_dst);
	if (!dwd) {
		return ERROR_ENOENT;
	}
	if (dwd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
#ifndef METARESTORE
	if (!fsnodes_access(dwd,uid,gid,MODE_MASK_W)) {
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
//		name_dst = fp->name;
	}
	de = fsnodes_lookup(dwd,nleng_dst,name_dst);
	if (de) {
		if (de->child->type==TYPE_DIRECTORY) {
			return ERROR_EPERM; // ISDIR
		}
		fsnodes_unlink(ts,de);
	}
	fsnodes_remove_edge(ts,se);
	fsnodes_link(ts,dwd,node,nleng_dst,name_dst);
#ifndef METARESTORE
	changelog(version++,"%u|MOVE(%u,%s,%u,%s):%u",main_time(),parent_src,fsnodes_escape_name(nleng_src,name_src),parent_dst,fsnodes_escape_name(nleng_dst,name_dst),node->id);
	stats_rename++;
#else
	version++;
#endif
	return STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_link(uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,uint8_t *name_dst,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]) {
	uint32_t ts;
#else
uint8_t fs_link(uint32_t ts,uint32_t inode_src,uint32_t parent_dst,uint32_t nleng_dst,uint8_t *name_dst) {
#endif
	fsnode *sp;
	fsnode *dwd;
#ifndef METARESTORE
	ts = main_time();
//	*inode = 0;
//	memset(attr,0,35);
#endif
	sp = fsnodes_id_to_node(inode_src);
	if (!sp) {
		return ERROR_ENOENT;
	}
	if (sp->type==TYPE_TRASH || sp->type==TYPE_RESERVED) {
		return ERROR_ENOENT;
	}
	if (sp->type==TYPE_DIRECTORY) {
		return ERROR_EPERM;
	}
	dwd = fsnodes_id_to_node(parent_dst);
	if (!dwd) {
		return ERROR_ENOENT;
	}
	if (dwd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
#ifndef METARESTORE
	if (!fsnodes_access(dwd,uid,gid,MODE_MASK_W)) {
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
	fsnodes_fill_attr(sp,attr);
	changelog(version++,"%u|LINK(%u,%u,%s)",main_time(),inode_src,parent_dst,fsnodes_escape_name(nleng_dst,name_dst));
#else
	version++;
#endif
	return STATUS_OK;
}


#ifndef METARESTORE
uint8_t fs_append(uint32_t inode,uint32_t inode_src,uint32_t uid,uint32_t gid) {
	uint32_t ts;
#else
uint8_t fs_append(uint32_t ts,uint32_t inode,uint32_t inode_src) {
#endif
	uint8_t status;
	fsnode *p,*fp;
	if (inode==inode_src) {
		return ERROR_EINVAL;
	}
	fp = fsnodes_id_to_node(inode_src);
	if (!fp) {
		return ERROR_ENOENT;
	}
	if (fp->type!=TYPE_FILE && fp->type!=TYPE_TRASH && fp->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
#ifndef METARESTORE
	if (!fsnodes_access(fp,uid,gid,MODE_MASK_R)) {
		return ERROR_EACCES;
	}
#endif
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
#ifndef METARESTORE
	if (!fsnodes_access(p,uid,gid,MODE_MASK_W)) {
		return ERROR_EACCES;
	}
	ts = main_time();
#endif
	status = fsnodes_appendchunks(ts,p,fp);
	if (status!=STATUS_OK) {
		return status;
	}
#ifndef METARESTORE
	changelog(version++,"%u|APPEND(%u,%u)",ts,inode,inode_src);
#else
	version++;
#endif
	return STATUS_OK;
}

#ifndef METARESTORE
uint8_t fs_readdir_size(uint32_t inode,uint32_t uid,uint32_t gid,void **dnode,uint32_t *dbuffsize,uint8_t packed) {
	fsnode *p;
	*dnode = NULL;
	*dbuffsize = 0;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if (!fsnodes_access(p,uid,gid,MODE_MASK_R)) {
		return ERROR_EACCES;
	}
	*dnode = p;
	*dbuffsize = fsnodes_getdirsize(p,packed);
	return STATUS_OK;
}

void fs_readdir_data(void *dnode,uint8_t *dbuff,uint8_t packed) {
	fsnode *p = (fsnode*)dnode;
	changelog(version++,"%u|ACCESS(%u)",main_time(),p->id);
	fsnodes_getdirdata(main_time(),p,dbuff,packed);
	stats_readdir++;
}


uint8_t fs_checkfile(uint32_t inode,uint16_t chunkcount[256]) {
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	fsnodes_checkfile(p,chunkcount);
	return STATUS_OK;
}

uint8_t fs_opencheck(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t flags) {
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
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
		if (!fsnodes_access(p,uid,gid,modemask)) {
			return ERROR_EACCES;
		}
	}
	stats_open++;
	return STATUS_OK;
}
#endif


uint8_t fs_aquire(uint32_t inode,uint32_t cuid) {
	fsnode *p;
	cuidrec *cr;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	for (cr=p->data.fdata.cuids ; cr ; cr=cr->next) {
		if (cr->cuid==cuid) {
			return ERROR_EINVAL;
		}
	}
	cr = (cuidrec*)malloc(sizeof(cuidrec));
	cr->cuid = cuid;
	cr->next = p->data.fdata.cuids;
	p->data.fdata.cuids = cr;
#ifndef METARESTORE
	changelog(version++,"%u|AQUIRE(%u,%u)",main_time(),inode,cuid);
#else
	version++;
#endif
	return STATUS_OK;
}

uint8_t fs_release(uint32_t inode,uint32_t cuid) {
	fsnode *p;
	cuidrec *cr,**crp;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	crp = &(p->data.fdata.cuids);
	while ((cr=*crp)) {
		if (cr->cuid==cuid) {
			*crp = cr->next;
			free(cr);
#ifndef METARESTORE
			changelog(version++,"%u|RELEASE(%u,%u)",main_time(),inode,cuid);
#else
			version++;
#endif
			return STATUS_OK;
		} else {
			crp = &(cr->next);
		}
	}
#ifndef METARESTORE
	syslog(LOG_WARNING,"release: customer not found");
#endif
	return ERROR_EINVAL;
}

#ifndef METARESTORE
uint32_t fs_newcuid(void) {
	changelog(version++,"%u|CUSTOMER():%u",main_time(),nextcuid);
	return nextcuid++;
}
#else
uint8_t fs_customer(uint32_t cuid) {
	if (cuid!=nextcuid) {
		return ERROR_MISMATCH;
	}
	version++;
	nextcuid++;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint64_t *chunkid,uint64_t *length) {
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
	p->atime = main_time();
	changelog(version++,"%u|ACCESS(%u)",main_time(),inode);
	stats_read++;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint8_t fs_writechunk(uint32_t inode,uint32_t indx,uint64_t *chunkid,uint64_t *length) {
	int status;
	uint32_t i;
	uint64_t ochunkid,nchunkid;
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
		for (i=p->data.fdata.chunks ; i<newsize ; i++) {
			p->data.fdata.chunktab[i]=0;
		}
		p->data.fdata.chunks = newsize;
	}
	ochunkid = p->data.fdata.chunktab[indx];
	status = chunk_multi_modify(&nchunkid,ochunkid,inode,indx,p->goal);
/* zapis bez zwiekszania wersji
	if (nchunkid==ochunkid && status==255) {
		*chunkid = nchunkid;
		*length = p->data.fdata.length;
		stats_write++;
		return 255;
	}
*/
	if (status!=STATUS_OK) {
		return status;
	}
	p->data.fdata.chunktab[indx] = nchunkid;
	*chunkid = nchunkid;
	*length = p->data.fdata.length;
	changelog(version++,"%u|WRITE(%u,%u):%llu",main_time(),inode,indx,nchunkid);
	p->mtime = p->ctime = main_time();
	stats_write++;
	return STATUS_OK;
}
#else
uint8_t fs_write(uint32_t ts,uint32_t inode,uint32_t indx,uint64_t chunkid) {
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
		for (i=p->data.fdata.chunks ; i<newsize ; i++) {
			p->data.fdata.chunktab[i]=0;
		}
		p->data.fdata.chunks = newsize;
	}
	ochunkid = p->data.fdata.chunktab[indx];
	status = chunk_multi_modify(ts,&nchunkid,ochunkid,inode,indx,p->goal);
	if (status!=STATUS_OK) {
		return status;
	}
	if (nchunkid!=chunkid) {
		return ERROR_MISMATCH;
	}
	p->data.fdata.chunktab[indx] = nchunkid;
	version++;
	p->mtime = p->ctime = ts;
	return STATUS_OK;
}
#endif

#ifndef METARESTORE
uint8_t fs_reinitchunk(uint32_t inode,uint32_t indx,uint64_t *chunkid) {
	int status;
	uint64_t nchunkid;
	fsnode *p;
	*chunkid = 0;
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
	nchunkid = p->data.fdata.chunktab[indx];
	if (nchunkid==0) {
		return ERROR_NOCHUNK;
	}
	status = chunk_multi_reinitialize(nchunkid);
	if (status!=STATUS_OK) {
		return status;
	}
	changelog(version++,"%u|REINIT(%u,%u):%llu",main_time(),inode,indx,nchunkid);
	*chunkid = nchunkid;
	p->mtime = p->ctime = main_time();
	return STATUS_OK;
}
#else
uint8_t fs_reinit(uint32_t ts,uint32_t inode,uint32_t indx,uint64_t chunkid) {
	int status;
	uint64_t nchunkid;
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
	if (indx>=p->data.fdata.chunks) {
		return ERROR_NOCHUNK;
	}
	nchunkid = p->data.fdata.chunktab[indx];
	if (nchunkid==0) {
		return ERROR_NOCHUNK;
	}
	if (chunkid != nchunkid) {
		return ERROR_MISMATCH;
	}
	status = chunk_multi_reinitialize(ts,nchunkid);
	if (status!=STATUS_OK) {
		return status;
	}
	version++;
	p->mtime = p->ctime = ts;
	return STATUS_OK;
}
#endif


#ifndef METARESTORE
uint8_t fs_writeend(uint32_t inode,uint64_t length,uint64_t chunkid) {
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
			p->mtime = p->ctime = main_time();
			changelog(version++,"%u|LENGTH(%u,%llu)",main_time(),inode,length);
		}
	}
	changelog(version++,"%u|UNLOCK(%llu)",main_time(),chunkid);
	return chunk_unlock(chunkid);
}
#endif

#ifndef METARESTORE
void fs_incversion(uint64_t chunkid) {
	changelog(version++,"%u|INCVERSION(%llu)",main_time(),chunkid);
}
#else
uint8_t fs_incversion(uint64_t chunkid) {
	version++;
	return chunk_increase_version(chunkid);
}
#endif

#ifndef METARESTORE
uint8_t fs_getgoal(uint32_t inode,uint8_t gmode,uint32_t fgtab[10],uint32_t dgtab[10]) {
	fsnode *p;
	memset(fgtab,0,10*sizeof(uint32_t));
	memset(dgtab,0,10*sizeof(uint32_t));
	if (!GMODE_ISVALID(gmode)) {
		return ERROR_EINVAL;
	}
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	if (p->type!=TYPE_DIRECTORY || gmode==GMODE_NORMAL) {
		if (p->goal>9) {
			syslog(LOG_WARNING,"inode %u: goal>9 !!! - fixing",inode);
			p->goal=9;
			if (p->type!=TYPE_DIRECTORY) {
				fsnodes_setchunksgoal(p);
			}
		} else if (p->goal<1) {
			syslog(LOG_WARNING,"inode %u: goal<1 !!! - fixing",inode);
			p->goal=1;
			if (p->type!=TYPE_DIRECTORY) {
				fsnodes_setchunksgoal(p);
			}
		}
		if (p->type==TYPE_DIRECTORY) {
			dgtab[p->goal]=1;
		} else {
			fgtab[p->goal]=1;
		}
		return STATUS_OK;
	}
	fsnodes_getgoal_recursive(p,fgtab,dgtab);
	return STATUS_OK;
}

uint8_t fs_gettrashtime_prepare(uint32_t inode,uint8_t gmode,void **fptr,void **dptr,uint32_t *fnodes,uint32_t *dnodes) {
	fsnode *p;
	bstnode *froot,*droot;
	froot = NULL;
	droot = NULL;
	*fptr = NULL;
	*dptr = NULL;
	*fnodes = 0;
	*dnodes = 0;
	if (!GMODE_ISVALID(gmode)) {
		return ERROR_EINVAL;
	}
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	if (p->type!=TYPE_DIRECTORY || gmode==GMODE_NORMAL) {
		if (p->type==TYPE_DIRECTORY) {
			fsnodes_bst_add(&droot,p->trashtime);
		} else {
			fsnodes_bst_add(&froot,p->trashtime);
		}
	} else {
		fsnodes_gettrashtime_recursive(p,&froot,&droot);
	}
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

#endif

#ifndef METARESTORE
uint8_t fs_setgoal(uint32_t inode,uint32_t uid,uint8_t goal,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	uint32_t ts;
#else
uint8_t fs_setgoal(uint32_t ts,uint32_t inode,uint32_t uid,uint8_t goal,uint8_t smode,uint32_t sinodes,uint32_t ncinodes,uint32_t nsinodes) {
	uint32_t si,nci,nsi;
#endif
	uint8_t set;
	fsnode *p;

#ifndef METARESTORE
	ts = main_time();
	*sinodes = 0;
	*ncinodes = 0;
	*nsinodes = 0;
#else
	si = 0;
	nci = 0;
	nsi = 0;
#endif
	if (!SMODE_ISVALID(smode) || goal>9 || goal<1) {
		return ERROR_EINVAL;
	}
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}

	if ((smode & SMODE_RMASK)==0 || p->type!=TYPE_DIRECTORY) {
		if (uid!=0 && p->uid!=uid) {
			return ERROR_EPERM;
		}
		set=0;
		switch (smode&SMODE_TMASK) {
		case SMODE_SET:
			if (p->goal!=goal) {
				p->goal=goal;
				set=1;
			}
			break;
		case SMODE_INCREASE:
			if (p->goal<goal) {
				p->goal=goal;
				set=1;
			}
			break;
		case SMODE_DECREASE:
			if (p->goal>goal) {
				p->goal=goal;
				set=1;
			}
			break;
		}
		if (set) {
#ifndef METARESTORE
			(*sinodes)++;
#else
			si++;
#endif
			if (p->type!=TYPE_DIRECTORY) {
				fsnodes_setchunksgoal(p);
			}
		} else {
#ifndef METARESTORE
			(*ncinodes)++;
#else
			nci++;
#endif
		}
		p->ctime = ts;
	} else {
#ifndef METARESTORE
		fsnodes_setgoal_recursive(p,ts,uid,goal,(smode&SMODE_TMASK),sinodes,ncinodes,nsinodes);
#else
		fsnodes_setgoal_recursive(p,ts,uid,goal,(smode&SMODE_TMASK),&si,&nci,&nsi);
#endif
	}
	
#ifndef METARESTORE
	changelog(version++,"%u|SETGOAL(%u,%u,%u,%u):%u,%u,%u",ts,inode,uid,goal,smode,*sinodes,*ncinodes,*nsinodes);
	return STATUS_OK;
#else
	version++;
	if (sinodes!=si || ncinodes!=nci || nsinodes!=nsi) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
#endif
}

#ifndef METARESTORE
uint8_t fs_settrashtime(uint32_t inode,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	uint32_t ts;
#else
uint8_t fs_settrashtime(uint32_t ts,uint32_t inode,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t sinodes,uint32_t ncinodes,uint32_t nsinodes) {
	uint32_t si,nci,nsi;
#endif
	uint8_t set;
	fsnode *p;

#ifndef METARESTORE
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
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}

	if ((smode & SMODE_RMASK)==0 || p->type!=TYPE_DIRECTORY) {
		if (uid!=0 && p->uid!=uid) {
			return ERROR_EPERM;
		}
		set=0;
		switch (smode&SMODE_TMASK) {
		case SMODE_SET:
			if (p->trashtime!=trashtime) {
				p->trashtime=trashtime;
				set=1;
			}
			break;
		case SMODE_INCREASE:
			if (p->trashtime<trashtime) {
				p->trashtime=trashtime;
				set=1;
			}
			break;
		case SMODE_DECREASE:
			if (p->trashtime>trashtime) {
				p->trashtime=trashtime;
				set=1;
			}
			break;
		}
		if (set) {
#ifndef METARESTORE
			(*sinodes)++;
#else
			si++;
#endif
		} else {
#ifndef METARESTORE
			(*ncinodes)++;
#else
			nci++;
#endif
		}
		p->ctime = ts;
	} else {
#ifndef METARESTORE
		fsnodes_settrashtime_recursive(p,ts,uid,trashtime,(smode&SMODE_TMASK),sinodes,ncinodes,nsinodes);
#else
		fsnodes_settrashtime_recursive(p,ts,uid,trashtime,(smode&SMODE_TMASK),&si,&nci,&nsi);
#endif
	}
	
#ifndef METARESTORE
	changelog(version++,"%u|SETTRASHTIME(%u,%u,%u,%u):%u,%u,%u",ts,inode,uid,trashtime,smode,*sinodes,*ncinodes,*nsinodes);
	return STATUS_OK;
#else
	version++;
	if (sinodes!=si || ncinodes!=nci || nsinodes!=nsi) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
#endif
}



#ifndef METARESTORE
uint8_t fs_get_dir_stats(uint32_t inode,uint32_t *inodes,uint32_t *dirs,uint32_t *files,uint32_t *ugfiles,uint32_t *mfiles,uint32_t *chunks,uint32_t *ugchunks,uint32_t *mchunks,uint64_t *length,uint64_t *size,uint64_t *gsize) {
	fsnode *p;
	p = fsnodes_id_to_node(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_RESERVED) {
		return ERROR_EPERM;
	}
	*ugfiles = 0;
	*mfiles = 0;
	*chunks = 0;
	*ugchunks = 0;
	*mchunks = 0;
	*length = 0ULL;
	*size = 0ULL;
	*gsize = 0ULL;
	if (p->type==TYPE_FILE || p->type==TYPE_TRASH || p->type==TYPE_RESERVED) {
		*inodes = 1;
		*dirs = 0;
		*files = 1;
		fsnodes_get_file_stats(p,ugfiles,mfiles,chunks,ugchunks,mchunks,length,size,gsize);
	} else if (p->type==TYPE_DIRECTORY) {
		*inodes = 1;
		*dirs = 1;
		*files = 0;
		fsnodes_get_dir_stats(p,inodes,dirs,files,ugfiles,mfiles,chunks,ugchunks,mchunks,length,size,gsize);
	}
	return STATUS_OK;
}
#endif

void fs_add_files_to_chunks() {
	uint32_t i,j;
	uint64_t chunkid;
	fsnode *f;
#ifndef METARESTORE
	syslog(LOG_NOTICE,"inodes: %u",nodes);
	syslog(LOG_NOTICE,"dirnodes: %u",dirnodes);
	syslog(LOG_NOTICE,"filenodes: %u",filenodes);
	syslog(LOG_NOTICE,"chunks: %u",chunk_count());
#endif
	for (i=0 ; i<NODEHASHSIZE ; i++) {
		for (f=nodehash[i] ; f ; f=f->next) {
			if (f->type==TYPE_FILE || f->type==TYPE_TRASH || f->type==TYPE_RESERVED) {
				for (j=0 ; j<f->data.fdata.chunks ; j++) {
					chunkid = f->data.fdata.chunktab[j];
					if (chunkid>0) {
						chunk_add_file(chunkid,f->id,j,f->goal);
					}
				}
			}
		}
	}
}

#ifndef METARESTORE

void fs_show_counts() {
	syslog(LOG_NOTICE,"inodes: %u",nodes);
	syslog(LOG_NOTICE,"dirnodes: %u",dirnodes);
	syslog(LOG_NOTICE,"filenodes: %u",filenodes);
	syslog(LOG_NOTICE,"chunks: %u",chunk_count());
	syslog(LOG_NOTICE,"chunks to delete: %u",chunk_todel_count());
}

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
		syslog(LOG_ERR,"structure error - %s inconsistency (edge: %u,%s -> %u)",iname,e->parent->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
		if (leng<size) {
			leng += snprintf(buff+leng,size-leng,"structure error - %s inconsistency (edge: %u,%s -> %u)\n",iname,e->parent->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
		}
	} else {
		if (e->child->type==TYPE_TRASH) {
			syslog(LOG_ERR,"structure error - %s inconsistency (edge: TRASH,%s -> %u)",iname,fsnodes_escape_name(e->nleng,e->name),e->child->id);
			if (leng<size) {
				leng += snprintf(buff+leng,size-leng,"structure error - %s inconsistency (edge: TRASH,%s -> %u)\n",iname,fsnodes_escape_name(e->nleng,e->name),e->child->id);
			}
		} else if (e->child->type==TYPE_RESERVED) {
			syslog(LOG_ERR,"structure error - %s inconsistency (edge: RESERVED,%s -> %u)",iname,fsnodes_escape_name(e->nleng,e->name),e->child->id);
			if (leng<size) {
				leng += snprintf(buff+leng,size-leng,"structure error - %s inconsistency (edge: RESERVED,%s -> %u)\n",iname,fsnodes_escape_name(e->nleng,e->name),e->child->id);
			}
		} else {
			syslog(LOG_ERR,"structure error - %s inconsistency (edge: NULL,%s -> %u)",iname,fsnodes_escape_name(e->nleng,e->name),e->child->id);
			if (leng<size) {
				leng += snprintf(buff+leng,size-leng,"structure error - %s inconsistency (edge: NULL,%s -> %u)\n",iname,fsnodes_escape_name(e->nleng,e->name),e->child->id);
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
	static char *msgbuff=NULL,*tmp;
	static uint32_t leng=0;
	fsnode *f;
	fsedge *e;

	if ((uint32_t)(main_time())<=starttime+150) {
		return;
	}
	if (i>=NODEHASHSIZE) {
		syslog(LOG_NOTICE,"structure check loop");
		i=0;
	}
	if (i==0) {
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
			fsinfo_msgbuff=malloc(MSGBUFFSIZE);
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
	for (k=0 ; k<32 && i<NODEHASHSIZE ; k++,i++) {
		for (f=nodehash[i] ; f ; f=f->next) {
			if (f->type==TYPE_FILE || f->type==TYPE_TRASH || f->type==TYPE_RESERVED) {
				valid = 1;
				ugflag = 0;
				for (j=0 ; j<f->data.fdata.chunks ; j++) {
					chunkid = f->data.fdata.chunktab[j];
					if (chunkid>0) {
						if (chunk_get_validcopies(chunkid,&vc)!=STATUS_OK) {
							syslog(LOG_ERR,"structure error - chunk %llu not found (file: %u ; index: %u)",chunkid,f->id,j);
							if (leng<MSGBUFFSIZE) {
								leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - chunk %llu not found (file: %u ; index: %u)\n",chunkid,f->id,j);
							}
							valid =0;
							mchunks++;
						} else if (vc==0) {
							syslog(LOG_ERR,"damaged chunk %llu (file: %u ; index: %u)",chunkid,f->id,j);
							if (leng<MSGBUFFSIZE) {
								leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"damaged chunk %llu (file: %u ; index: %u)\n",chunkid,f->id,j);
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
						syslog(LOG_ERR,"damaged file in trash %08X: %s",f->id,fsnodes_escape_name(f->parents->nleng,f->parents->name));
						if (leng<MSGBUFFSIZE) {
							leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"damaged file in trash %08X: %s\n",f->id,fsnodes_escape_name(f->parents->nleng,f->parents->name));
						}
					} else if (f->type==TYPE_RESERVED) {
						syslog(LOG_ERR,"damaged reserved file %08X: %s",f->id,fsnodes_escape_name(f->parents->nleng,f->parents->name));
						if (leng<MSGBUFFSIZE) {
							leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"damaged reserved file %08X: %s\n",f->id,fsnodes_escape_name(f->parents->nleng,f->parents->name));
						}
					} else {
						uint8_t *path;
						uint16_t pleng;
						for (e=f->parents ; e ; e=e->nextparent) {
							fsnodes_getpath(e,&pleng,&path);
							syslog(LOG_ERR,"damaged file %08X: %s",f->id,fsnodes_escape_name(pleng,path));
							if (leng<MSGBUFFSIZE) {
								leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"damaged file %08X: %s\n",f->id,fsnodes_escape_name(pleng,path));
							}
							free(path);
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
						syslog(LOG_ERR,"structure error - edge->child/child->edges (node: %u ; edge: %u,%s -> %u)",f->id,e->parent->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
						if (leng<MSGBUFFSIZE) {
							leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - edge->child/child->edges (node: %u ; edge: %u,%s -> %u)\n",f->id,e->parent->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
						}
					} else {
						syslog(LOG_ERR,"structure error - edge->child/child->edges (node: %u ; edge: NULL,%s -> %u)",f->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
						if (leng<MSGBUFFSIZE) {
							leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - edge->child/child->edges (node: %u ; edge: NULL,%s -> %u)\n",f->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
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
							syslog(LOG_ERR,"structure error - edge->parent/parent->edges (node: %u ; edge: %u,%s -> %u)",f->id,e->parent->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
							if (leng<MSGBUFFSIZE) {
								leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - edge->parent/parent->edges (node: %u ; edge: %u,%s -> %u)\n",f->id,e->parent->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
							}
						} else {
							syslog(LOG_ERR,"structure error - edge->parent/parent->edges (node: %u ; edge: NULL,%s -> %u)",f->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
							if (leng<MSGBUFFSIZE) {
								leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - edge->parent/parent->edges (node: %u ; edge: NULL,%s -> %u)\n",f->id,fsnodes_escape_name(e->nleng,e->name),e->child->id);
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
	changelog(version++,"%u|EMPTYTRASH():%u,%u",ts,fi,ri);
#else
	version++;
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
		if (p->data.fdata.cuids==NULL) {
			fsnodes_purge(ts,p);
			fi++;
		}
	}
#ifndef METARESTORE
	changelog(version++,"%u|EMPTYRESERVED():%u",ts,fi);
#else
	version++;
	if (freeinodes!=fi) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
#endif
}


#ifdef METARESTORE

uint64_t fs_getversion() {
	return version;
}

#endif

enum {FLAG_TREE,FLAG_TRASH,FLAG_RESERVED};

#ifdef METARESTORE
/* DUMP */

void fs_dumpedge(fsedge *e) {
	if (e->parent==NULL) {
		if (e->child->type==TYPE_TRASH) {
			printf("E|p:   TRASH|c:%08X|n:%s\n",e->child->id,fsnodes_escape_name(e->nleng,e->name));
		} else if (e->child->type==TYPE_RESERVED) {
			printf("E|p:RESERVED|c:%08X|n:%s\n",e->child->id,fsnodes_escape_name(e->nleng,e->name));
		} else {
			printf("E|p:    NULL|c:%08X|n:%s\n",e->child->id,fsnodes_escape_name(e->nleng,e->name));
		}
	} else {
		printf("E|p:%08X|c:%08X|n:%s\n",e->parent->id,e->child->id,fsnodes_escape_name(e->nleng,e->name));
	}
}

void fs_dumpnode(fsnode *f) {
	char c;
	uint32_t i,ch;
	cuidrec *cuidptr;

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
//	if (flag==FLAG_TRASH) {
//		c='T';
//	} else if (flag==FLAG_RESERVED) {
//		c='R';
//	}

	printf("%c|i:%08X|#:%d|m:%05o|u:%10u|g:%10u|a:%10u,m:%10u,c:%10u|t:%10u",c,f->id,f->goal,f->mode,f->uid,f->gid,f->atime,f->mtime,f->ctime,f->trashtime);

	if (f->type==TYPE_BLOCKDEV || f->type==TYPE_CHARDEV) {
		printf("|d:%5u,%5u\n",f->data.rdev>>16,f->data.rdev&0xFFFF);
	} else if (f->type==TYPE_SYMLINK) {
		printf("|p:%s\n",fsnodes_escape_name(f->data.sdata.pleng,f->data.sdata.path));
	} else if (f->type==TYPE_FILE || f->type==TYPE_TRASH || f->type==TYPE_RESERVED) {
		printf("|l:%20llu|c:(",f->data.fdata.length);
		ch = 0;
		for (i=0 ; i<f->data.fdata.chunks ; i++) {
			if (f->data.fdata.chunktab[i]!=0) {
				ch=i+1;
			}
		}
		for (i=0 ; i<ch ; i++) {
			if (f->data.fdata.chunktab[i]!=0) {
				printf("%016llX",f->data.fdata.chunktab[i]);
			} else {
				printf("N");
			}
			if (i+1<ch) {
				printf(",");
			}
		}
		printf(")|r:(");
		for (cuidptr=f->data.fdata.cuids ; cuidptr ; cuidptr=cuidptr->next) {
			printf("%u",cuidptr->cuid);
			if (cuidptr->next) {
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
		printf("I|i:%08X|f:%10u\n",n->id,n->ftime);
	}
}

void fs_dump(void) {
	fs_dumpnodes();
	fs_dumpedges(root);
	fs_dumpedgelist(trash);
	fs_dumpedgelist(reserved);
	fs_dumpfree();
}

#endif

void fs_storeedge(fsedge *e,FILE *fd) {
	uint8_t uedgebuff[4+4+2+65535];
	uint8_t *ptr;
	uint32_t t32;
	if (e==NULL) {	// last edge
		memset(uedgebuff,0,4+4+2);
		fwrite(uedgebuff,1,4+4+2,fd);
		return;
	}
	ptr = uedgebuff;
	if (e->parent==NULL) {
		t32 = 0;
	} else {
		t32 = e->parent->id;
	}
	ptr = uedgebuff;
	PUT32BIT(t32,ptr);
	t32 = e->child->id;
	PUT32BIT(t32,ptr);
	PUT16BIT(e->nleng,ptr);
	memcpy(ptr,e->name,e->nleng);
	fwrite(uedgebuff,1,4+4+2+e->nleng,fd);
}

int fs_loadedge(FILE *fd) {
	uint8_t uedgebuff[4+4+2];
	uint8_t *ptr;
	uint32_t parent_id;
	uint32_t child_id;
#ifdef EDGEHASH
	uint32_t hpos;
#endif
	fsedge *e;
	if (fread(uedgebuff,1,4+4+2,fd)!=4+4+2) {
#ifdef METARESTORE
		fprintf(stderr,"loading edge: read error: %s\n",strerror(errno));
#else
		syslog(LOG_ERR,"loading edge: read error: %m");
#endif
		return -1;
	}
	ptr = uedgebuff;
	GET32BIT(parent_id,ptr);
	GET32BIT(child_id,ptr);
	if (parent_id==0 && child_id==0) {	// last edge
		return 1;
	}
	e = malloc(sizeof(fsedge));
	if (e==NULL) {
#ifdef METARESTORE
		fprintf(stderr,"loading edge: edge alloc: out of memory\n");
#else
		syslog(LOG_ERR,"loading edge: edge alloc: out of memory");
#endif
		return -1;
	}
	GET16BIT(e->nleng,ptr);
	e->name = malloc(e->nleng);
	if (e->name==NULL) {
#ifdef METARESTORE
		fprintf(stderr,"loading edge: name alloc: out of memory\n");
#else
		syslog(LOG_ERR,"loading edge: name alloc: out of memory");
#endif
		free(e);
		return -1;
	}
	if (fread(e->name,1,e->nleng,fd)!=e->nleng) {
#ifdef METARESTORE
		fprintf(stderr,"loading edge: read error: %s\n",strerror(errno));
#else
		syslog(LOG_ERR,"loading edge: read error: %m");
#endif
		free(e->name);
		free(e);
		return -1;
	}
	e->child = fsnodes_id_to_node(child_id);
	if (e->child==NULL) {
#ifdef METARESTORE
		fprintf(stderr,"loading edge: %u,%s->%u error: child not found\n",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
#else
		syslog(LOG_ERR,"loading edge: %u,%s->%u error: child not found",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
#endif
		free(e->name);
		free(e);
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
#ifdef METARESTORE
			fprintf(stderr,"loading edge: %u,%s->%u error: bad child type (%c)\n",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id,e->child->type);
#else
			syslog(LOG_ERR,"loading edge: %u,%s->%u error: bad child type (%c)",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id,e->child->type);
#endif
			free(e->name);
			free(e);
			return -1;
		}
	} else {
		e->parent = fsnodes_id_to_node(parent_id);
		if (e->parent==NULL) {
#ifdef METARESTORE
			fprintf(stderr,"loading edge: %u,%s->%u error: parent not found\n",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
#else
			syslog(LOG_ERR,"loading edge: %u,%s->%u error: parent not found",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id);
#endif
			free(e->name);
			free(e);
			return -1;
		}
		if (e->parent->type!=TYPE_DIRECTORY) {
#ifdef METARESTORE
			fprintf(stderr,"loading edge: %u,%s->%u error: bad parent type (%c)\n",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id,e->parent->type);
#else
			syslog(LOG_ERR,"loading edge: %u,%s->%u error: bad parent type (%c)",parent_id,fsnodes_escape_name(e->nleng,e->name),child_id,e->parent->type);
#endif
			free(e->name);
			free(e);
			return -1;
		}
		e->nextchild = e->parent->data.ddata.children;
		if (e->nextchild) {
			e->nextchild->prevchild = &(e->nextchild);
		}
		e->parent->data.ddata.children = e;
		e->prevchild = &(e->parent->data.ddata.children);
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
	return 0;
}

void fs_storenode(fsnode *f,FILE *fd) {
	uint8_t unodebuff[1+4+1+2+4+4+4+4+4+4+8+4+2+8*MAX_CHUNKS_PER_FILE+4*65536+4];
	uint8_t *ptr,t8;
	uint32_t t32,ch;
	uint64_t t64;
	uint16_t t16;
	cuidrec *cuidptr;

	if (f==NULL) {	// last node
		fputc(0,fd);
		return;
	}
	ptr = unodebuff;
	t8 = f->type;
	PUT8BIT(t8,ptr);
	t32 = f->id;
	PUT32BIT(t32,ptr);
	t8 = f->goal;
	PUT8BIT(t8,ptr);
	t16 = f->mode;
	PUT16BIT(t16,ptr);
	t32 = f->uid;
	PUT32BIT(t32,ptr);
	t32 = f->gid;
	PUT32BIT(t32,ptr);
	t32 = f->atime;
	PUT32BIT(t32,ptr);
	t32 = f->mtime;
	PUT32BIT(t32,ptr);
	t32 = f->ctime;
	PUT32BIT(t32,ptr);
	t32 = f->trashtime;
	PUT32BIT(t32,ptr);
	switch (f->type) {
	case TYPE_DIRECTORY:
	case TYPE_SOCKET:
	case TYPE_FIFO:
		fwrite(unodebuff,1,1+4+1+2+4+4+4+4+4+4,fd);
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		t32 = f->data.rdev;
		PUT32BIT(t32,ptr);
		fwrite(unodebuff,1,1+4+1+2+4+4+4+4+4+4+4,fd);
		break;
	case TYPE_SYMLINK:
		t32 = f->data.sdata.pleng;
		PUT32BIT(t32,ptr);
		fwrite(unodebuff,1,1+4+1+2+4+4+4+4+4+4+4,fd);
		fwrite(f->data.sdata.path,1,t32,fd);
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		t64 = f->data.fdata.length;
		PUT64BIT(t64,ptr);
		ch = 0;
		for (t32=0 ; t32<f->data.fdata.chunks ; t32++) {
			if (f->data.fdata.chunktab[t32]!=0) {
				ch=t32+1;
			}
		}
		PUT32BIT(ch,ptr);
		t16=0;
		for (cuidptr=f->data.fdata.cuids ; cuidptr && t16<65535; cuidptr=cuidptr->next) {
			t16++;
		}
		PUT16BIT(t16,ptr);

		for (t32=0 ; t32<ch ; t32++) {
			t64 = f->data.fdata.chunktab[t32];
			PUT64BIT(t64,ptr);
		}

		t16=0;
		for (cuidptr=f->data.fdata.cuids ; cuidptr && t16<65535; cuidptr=cuidptr->next) {
			t32 = cuidptr->cuid;
			PUT32BIT(t32,ptr);
			t16++;
		}

		fwrite(unodebuff,1,1+4+1+2+4+4+4+4+4+4+8+4+2+8*ch+4*t16,fd);
	}
}

int fs_loadnode(FILE *fd) {
	uint8_t unodebuff[4+1+2+4+4+4+4+4+4+8+4+2+8*MAX_CHUNKS_PER_FILE+4*65536+4];
	uint8_t *ptr,t8,type;
	uint32_t t32,ch;
	uint64_t t64;
	uint16_t t16;
	fsnode *p;
	cuidrec *cuidptr;
	uint32_t nodepos;

	type = fgetc(fd);
	if (type==0) {	// last node
		return 1;
	}
	p = malloc(sizeof(fsnode));
	if (p==NULL) {
#ifdef METARESTORE
		fprintf(stderr,"loading node: node alloc: out of memory\n");
#else
		syslog(LOG_ERR,"loading node: node alloc: out of memory");
#endif
		return -1;
	}
	p->type = type;
	switch (type) {
	case TYPE_DIRECTORY:
	case TYPE_FIFO:
	case TYPE_SOCKET:
		if (fread(unodebuff,1,4+1+2+4+4+4+4+4+4,fd)!=4+1+2+4+4+4+4+4+4) {
#ifdef METARESTORE
			fprintf(stderr,"loading node: read error: %s\n",strerror(errno));
#else
			syslog(LOG_ERR,"loading node: read error: %m");
#endif
			free(p);
			return -1;
		}
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
	case TYPE_SYMLINK:
		if (fread(unodebuff,1,4+1+2+4+4+4+4+4+4+4,fd)!=4+1+2+4+4+4+4+4+4+4) {
#ifdef METARESTORE
			fprintf(stderr,"loading node: read error: %s\n",strerror(errno));
#else
			syslog(LOG_ERR,"loading node: read error: %m");
#endif
			free(p);
			return -1;
		}
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		if (fread(unodebuff,1,4+1+2+4+4+4+4+4+4+8+4+2,fd)!=4+1+2+4+4+4+4+4+4+8+4+2) {
#ifdef METARESTORE
			fprintf(stderr,"loading node: read error: %s\n",strerror(errno));
#else
			syslog(LOG_ERR,"loading node: read error: %m");
#endif
			free(p);
			return -1;
		}
		break;
	default:
#ifdef METARESTORE
		fprintf(stderr,"loading node: unrecognized node type: %c\n",type);
#else
		syslog(LOG_ERR,"loading node: unrecognized node type: %c",type);
#endif
		free(p);
		return -1;
	}
	ptr = unodebuff;
	GET32BIT(t32,ptr);
	p->id = t32;
	GET8BIT(t8,ptr);
	p->goal = t8;
	GET16BIT(t16,ptr);
	p->mode = t16;
	GET32BIT(t32,ptr);
	p->uid = t32;
	GET32BIT(t32,ptr);
	p->gid = t32;
	GET32BIT(t32,ptr);
	p->atime = t32;
	GET32BIT(t32,ptr);
	p->mtime = t32;
	GET32BIT(t32,ptr);
	p->ctime = t32;
	GET32BIT(t32,ptr);
	p->trashtime = t32;
	switch (type) {
	case TYPE_DIRECTORY:
		p->data.ddata.children = NULL;
		p->data.ddata.nlink = 2;
		p->data.ddata.elements = 0;
	case TYPE_SOCKET:
	case TYPE_FIFO:
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		GET32BIT(t32,ptr);
		p->data.rdev = t32;
		break;
	case TYPE_SYMLINK:
		GET32BIT(t32,ptr);
		p->data.sdata.pleng = t32;
		if (t32>0) {
			p->data.sdata.path = malloc(t32);
			if (p->data.sdata.path==NULL) {
#ifdef METARESTORE
				fprintf(stderr,"loading node: path alloc: out of memory\n");
#else
				syslog(LOG_ERR,"loading node: path alloc: out of memory");
#endif
				free(p);
				return -1;
			}
			if (fread(p->data.sdata.path,1,t32,fd)!=t32) {
#ifdef METARESTORE
				fprintf(stderr,"loading node: read error: %s\n",strerror(errno));
#else
				syslog(LOG_ERR,"loading node: read error: %m");
#endif
				free(p->data.sdata.path);
				free(p);
				return -1;
			}	
		} else {
			p->data.sdata.path = NULL;
		}
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		GET64BIT(t64,ptr);
		p->data.fdata.length = t64;
		GET32BIT(ch,ptr);
		p->data.fdata.chunks = ch;
		GET16BIT(t16,ptr);
		if (ch>0) {
			p->data.fdata.chunktab = malloc(sizeof(uint64_t)*ch);
			if (p->data.fdata.chunktab==NULL) {
#ifdef METARESTORE
				fprintf(stderr,"loading node: chunktab alloc: out of memory\n");
#else
				syslog(LOG_ERR,"loading node: chunktab alloc: out of memory");
#endif
				free(p);
				return -1;
			}
		} else {
			p->data.fdata.chunktab = NULL;
		}
		if (fread(ptr,1,8*ch+4*t16,fd)!=8*ch+4*t16) {
#ifdef METARESTORE
			fprintf(stderr,"loading node: read error: %s\n",strerror(errno));
#else
			syslog(LOG_ERR,"loading node: read error: %m");
#endif
			if (p->data.fdata.chunktab) {
				free(p->data.fdata.chunktab);
			}
			free(p);
			return -1;
		}
		for (t32=0 ; t32<ch ; t32++) {
			GET64BIT(t64,ptr);
			p->data.fdata.chunktab[t32]=t64;
		}
		p->data.fdata.cuids=NULL;
		while (t16) {
			GET32BIT(t32,ptr);
			cuidptr = malloc(sizeof(cuidrec));
			if (cuidptr==NULL) {
#ifdef METARESTORE
				fprintf(stderr,"loading node: cuidrec alloc: out of memory\n");
#else
				syslog(LOG_ERR,"loading node: cuidrec alloc: out of memory");
#endif
				if (p->data.fdata.chunktab) {
					free(p->data.fdata.chunktab);
				}
				free(p);
			}
			cuidptr->cuid = t32;
			cuidptr->next = p->data.fdata.cuids;
			p->data.fdata.cuids = cuidptr;
#ifndef METARESTORE
			matocuserv_init_customers(t32,p->id);
#endif
			t16--;
		}
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
	fs_storenode(NULL,fd);	// end marker
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
	fs_storeedge(NULL,fd);	// end marker
}

int fs_lostnode(fsnode *p) {
	uint8_t artname[40];
	uint32_t i,l;
	i=0;
	do {
		if (i==0) {
			l = snprintf((char*)artname,40,"lost_node_%u",p->id);
		} else {
			l = snprintf((char*)artname,40,"lost_node_%u.%u",p->id,i);
		}
		if (!fsnodes_nameisused(root,l,artname)) {
			fsnodes_link(0,root,p,l,artname);
			return 1;
		}
		i++;
	} while (i);
	return -1;
}

int fs_checknodes() {
	uint32_t i;
	fsnode *p;
	for (i=0 ; i<NODEHASHSIZE ; i++) {
		for (p=nodehash[i] ; p ; p=p->next) {
			if (p->parents==NULL && p!=root) {
#ifdef METARESTORE
				fprintf(stderr,"fschk: found lost inode: %08X\n",p->id);
#else
				syslog(LOG_ERR,"fschk: found lost inode: %08X",p->id);
#endif
				if (fs_lostnode(p)<0) {
					return -1;
				}
			}
		}
	}
	return 1;
}

int fs_loadnodes(FILE *fd) {
	int s;
	do {
		s = fs_loadnode(fd);
		if (s<0) {
			return -1;
		}
	} while (s==0);
	return 0;
}

int fs_loadedges(FILE *fd) {
	int s;
	do {
		s = fs_loadedge(fd);
		if (s<0) {
			return -1;
		}
	} while (s==0);
	return 0;
}

#if 0
void fs_storeuninode(int flag,fsnode *f,FILE *fd) {
	uint8_t unodebuff[4+2+MAXFNAMELENG+1+2+4+4+4+4+4+4+8+4+2+8*MAX_CHUNKS_PER_FILE+4*65536+4];
	uint8_t *ptr,t8;
	uint32_t t32,ch;
	uint64_t t64;
	uint16_t t16;
	cuidrec *cuidptr;

	ptr = unodebuff;
	t32 = f->id;
	PUT32BIT(t32,ptr);
	t16 = f->nleng;
	PUT16BIT(t16,ptr);
	memcpy(ptr,f->name,f->nleng);
	ptr+=f->nleng;
	t8 = f->goal;
	PUT8BIT(t8,ptr);
	t16 = f->mode;
	PUT16BIT(t16,ptr);
	t32 = f->uid;
	PUT32BIT(t32,ptr);
	t32 = f->gid;
	PUT32BIT(t32,ptr);
	t32 = f->atime;
	PUT32BIT(t32,ptr);
	t32 = f->mtime;
	PUT32BIT(t32,ptr);
	t32 = f->ctime;
	PUT32BIT(t32,ptr);
	t32 = f->trashtime;
	PUT32BIT(t32,ptr);
	switch (f->type) {
	case TYPE_DIRECTORY:
	case TYPE_SOCKET:
	case TYPE_FIFO:
		fwrite(unodebuff,1,4+2+f->nleng+1+2+4+4+4+4+4+4,fd);
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		t32 = f->data.rdev;
		PUT32BIT(t32,ptr);
		fwrite(unodebuff,1,4+2+f->nleng+1+2+4+4+4+4+4+4+4,fd);
		break;
	case TYPE_SYMLINK:
		t32 = f->data.sdata.pleng;
		PUT32BIT(t32,ptr);
		fwrite(unodebuff,1,4+2+f->nleng+1+2+4+4+4+4+4+4+4,fd);
		fwrite(f->data.sdata.path,1,t32,fd);
		break;
	case TYPE_FILE:
		t64 = f->data.fdata.length;
		PUT64BIT(t64,ptr);
		ch = 0;
		for (t32=0 ; t32<f->data.fdata.chunks ; t32++) {
			if (f->data.fdata.chunktab[t32]!=0) {
				ch=t32+1;
			}
		}
		PUT32BIT(ch,ptr);
		t16=0;
		for (cuidptr=f->data.fdata.cuids ; cuidptr && t16<65535; cuidptr=cuidptr->next) {
			t16++;
		}
		PUT16BIT(t16,ptr);

		for (t32=0 ; t32<ch ; t32++) {
			t64 = f->data.fdata.chunktab[t32];
			PUT64BIT(t64,ptr);
		}

		t16=0;
		for (cuidptr=f->data.fdata.cuids ; cuidptr && t16<65535; cuidptr=cuidptr->next) {
			t32 = cuidptr->cuid;
			PUT32BIT(t32,ptr);
			t16++;
		}

		if (flag==FLAG_TRASH) {
			t32 = f->data.fdata.pleng;
			PUT32BIT(t32,ptr);
			fwrite(unodebuff,1,4+2+f->nleng+1+2+4+4+4+4+4+4+8+4+2+8*ch+4*t16+4,fd);
			fwrite(f->data.fdata.path,1,t32,fd);
		} else {
			fwrite(unodebuff,1,4+2+f->nleng+1+2+4+4+4+4+4+4+8+4+2+8*ch+4*t16,fd);
		}
	}
}
#endif

fsnode* fs_loaduninode_1_4(int flag,uint8_t type,fsnode* node,FILE *fd) {
	uint8_t unodebuff[4+2+MAXFNAMELENG+1+2+4+4+4+4+4+4+8+4+2+8*MAX_CHUNKS_PER_FILE+4*65536+4];
	uint8_t *ptr,t8;
	uint32_t t32,ch;
	uint64_t t64;
	uint16_t t16;
	fsnode *p;
	fsedge *e;
	cuidrec *cuidptr;
	uint32_t nodepos;
#ifdef EDGEHASH
	uint32_t hpos;
#endif

	p = malloc(sizeof(fsnode));
	if (p==NULL) {
		return NULL;
	}
	e = malloc(sizeof(fsedge));
	if (e==NULL) {
		free(p);
		return NULL;
	}
	e->child = p;
	e->parent = node;
	e->nextparent = NULL;
	e->prevparent = &(p->parents);
	p->parents = e;

	p->type = type;
	fread(unodebuff,1,4+2,fd);
	ptr = unodebuff;
	GET32BIT(t32,ptr);
	p->id = t32;
	GET16BIT(t16,ptr);
	e->nleng = t16;
	e->name = malloc(t16);
	if (e->name==NULL) {
		free(p);
		free(e);
		return NULL;
	}
	switch (type) {
	case TYPE_DIRECTORY:
	case TYPE_FIFO:
	case TYPE_SOCKET:
		fread(unodebuff+6,1,e->nleng+1+2+4+4+4+4+4+4,fd);
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
	case TYPE_SYMLINK:
		fread(unodebuff+6,1,e->nleng+1+2+4+4+4+4+4+4+4,fd);
		break;
	case TYPE_FILE:
		fread(unodebuff+6,1,e->nleng+1+2+4+4+4+4+4+4+8+4+2,fd);
		break;
	default:
		free(p);
		free(e->name);
		free(e);
		return NULL;
	}

	memcpy(e->name,ptr,e->nleng);
	ptr+=e->nleng;

	GET8BIT(t8,ptr);
	p->goal = t8;
	GET16BIT(t16,ptr);
	p->mode = t16;
	GET32BIT(t32,ptr);
	p->uid = t32;
	GET32BIT(t32,ptr);
	p->gid = t32;
	GET32BIT(t32,ptr);
	p->atime = t32;
	GET32BIT(t32,ptr);
	p->mtime = t32;
	GET32BIT(t32,ptr);
	p->ctime = t32;
	GET32BIT(t32,ptr);
	p->trashtime = t32;
	switch (type) {
	case TYPE_DIRECTORY:
		p->data.ddata.children = NULL;
		p->data.ddata.nlink = 2;
		p->data.ddata.elements = 0;
	case TYPE_SOCKET:
	case TYPE_FIFO:
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		GET32BIT(t32,ptr);
		p->data.rdev = t32;
		break;
	case TYPE_SYMLINK:
		GET32BIT(t32,ptr);
		p->data.sdata.pleng = t32;
		if (t32>0) {
			p->data.sdata.path = malloc(t32);
			if (p->data.sdata.path==NULL) {
				free(e->name);
				free(e);
				free(p);
				return NULL;
			}
			fread(p->data.sdata.path,1,t32,fd);
			while (t32>0 && p->data.sdata.path[t32-1]==0) {
				t32--;
			}
			p->data.sdata.pleng = t32;
		} else {
			p->data.sdata.path = NULL;
		}
		break;
	case TYPE_FILE:
		GET64BIT(t64,ptr);
		p->data.fdata.length = t64;
		GET32BIT(ch,ptr);
		p->data.fdata.chunks = ch;
		GET16BIT(t16,ptr);
		if (ch>0) {
			p->data.fdata.chunktab = malloc(sizeof(uint64_t)*ch);
		} else {
			p->data.fdata.chunktab = NULL;
		}
		if (flag==FLAG_TRASH) {
			fread(ptr,1,8*ch+4*t16+4,fd);
		} else if (ch>0 || t16>0) {
			fread(ptr,1,8*ch+4*t16,fd);
		}
		for (t32=0 ; t32<ch ; t32++) {
			GET64BIT(t64,ptr);
			p->data.fdata.chunktab[t32]=t64;
		}
		p->data.fdata.cuids=NULL;
		while (t16) {
			GET32BIT(t32,ptr);
			cuidptr = malloc(sizeof(cuidrec));
			cuidptr->cuid = t32;
			cuidptr->next = p->data.fdata.cuids;
			p->data.fdata.cuids = cuidptr;
#ifndef METARESTORE
			matocuserv_init_customers(t32,p->id);
#endif
			t16--;
		}
		if (flag==FLAG_TRASH) {
			GET32BIT(t32,ptr);
			if (t32>0) {
				uint8_t *tmpname;
				tmpname = malloc(t32+e->nleng+1);
				if (tmpname==NULL) {
					free(e->name);
					free(e);
					free(p);
					return NULL;
				}
				fread(tmpname,1,t32,fd);
				while (t32>0 && tmpname[t32-1]==0) {
					t32--;
				}
				tmpname[t32]='/';
				memcpy(tmpname+t32+1,e->name,e->nleng);
				free(e->name);
				e->name = tmpname;
				e->nleng+=t32+1;
			}
		}
	}

	if (flag==FLAG_TREE) {
//		p->parent = node;
		node->data.ddata.elements++;
		if (type==TYPE_DIRECTORY) {
			node->data.ddata.nlink++;
		}
		e->nextchild = node->data.ddata.children;
		if (e->nextchild) {
			e->nextchild->prevchild = &(e->nextchild);
		}
		node->data.ddata.children = e;
		e->prevchild = &(node->data.ddata.children);
#ifdef EDGEHASH
		hpos = EDGEHASHPOS(fsnodes_hash(e->parent->id,e->nleng,e->name));
		e->next = edgehash[hpos];
		if (e->next) {
			e->next->prev = &(e->next);
		}
		edgehash[hpos] = e;
		e->prev = &(edgehash[hpos]);
#endif
	} else if (flag==FLAG_TRASH) {
//		p->parent = NULL;
		p->type = TYPE_TRASH;
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
		trashspace += p->data.fdata.length;
		trashnodes++;
	} else {	// flag==FLAG_RESERVED
//		p->parent = NULL;
		p->type = TYPE_RESERVED;
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
		reservedspace += p->data.fdata.length;
		reservednodes++;
	}
//	p->parents = NULL;
	nodepos = NODEHASHPOS(p->id);
	p->next = nodehash[nodepos];
	nodehash[nodepos] = p;
	fsnodes_used_inode(p->id);
	nodes++;
	if (type==TYPE_DIRECTORY) {
		dirnodes++;
	}
	if (type==TYPE_FILE) {
		filenodes++;
	}
	return p;
}

#ifndef METARESTORE
/*
fsnode* fs_loaduninode_1_3(uint8_t type,fsnode* node,FILE *fd) {
	uint8_t unodebuff[4+2+MAXFNAMELENG+1+2+4+4+4+4+4+8+2+8*65536+4];
	uint8_t *ptr,t8;
	uint32_t t32;
	uint64_t t64;
	uint16_t t16;
	fsnode *p;
	uint32_t nodepos;

	p = malloc(sizeof(fsnode));
	if (p==NULL) {
		return NULL;
	}
	p->type = type;
	fread(unodebuff,1,4+2,fd);
	ptr = unodebuff;
	GET32BIT(t32,ptr);
	p->id = t32;
	GET16BIT(t16,ptr);
	p->nleng = t16;
	p->name = malloc(t16);
	if (p->name==NULL) {
		free(p);
		return NULL;
	}
	switch (type) {
	case TYPE_DIRECTORY:
	case TYPE_FIFO:
	case TYPE_SOCKET:
		fread(unodebuff+6,1,p->nleng+1+2+4+4+4+4+4,fd);
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
	case TYPE_SYMLINK:
		fread(unodebuff+6,1,p->nleng+1+2+4+4+4+4+4+4,fd);
		break;
	case TYPE_FILE:
		fread(unodebuff+6,1,p->nleng+1+2+4+4+4+4+4+8+2,fd);
		break;
	default:
		return NULL;
	}

	memcpy(p->name,ptr,p->nleng);
	ptr+=p->nleng;
	GET8BIT(t8,ptr);
	p->goal = t8&0x0F;
	p->trashtime = (t8&0x10)?0:DEFAULT_TRASHTIME;
	GET16BIT(t16,ptr);
	p->mode = t16;
	GET32BIT(t32,ptr);
	p->uid = t32;
	GET32BIT(t32,ptr);
	p->gid = t32;
	GET32BIT(t32,ptr);
	p->atime = t32;
	GET32BIT(t32,ptr);
	p->mtime = t32;
	GET32BIT(t32,ptr);
	p->ctime = t32;
	switch (type) {
	case TYPE_DIRECTORY:
		p->data.ddata.children = NULL;
		p->data.ddata.nlink = 2;
		p->data.ddata.elements = 0;
	case TYPE_SOCKET:
	case TYPE_FIFO:
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		GET32BIT(t32,ptr);
		p->data.rdev = t32;
		break;
	case TYPE_SYMLINK:
		GET32BIT(t32,ptr);
		p->data.sdata.pleng = t32;
		if (t32>0) {
			p->data.sdata.path = malloc(t32);
			if (p->data.sdata.path==NULL) {
				free(p->name);
				free(p);
				return NULL;
			}
			fread(p->data.sdata.path,1,t32,fd);
		} else {
			p->data.sdata.path = NULL;
		}
		break;
	case TYPE_FILE:
		GET64BIT(t64,ptr);
		p->data.fdata.length = t64;
		GET16BIT(t16,ptr);
		p->data.fdata.chunks = t16;
		if (t16>0) {
			p->data.fdata.chunktab = malloc(sizeof(uint64_t)*t16);
		} else {
			p->data.fdata.chunktab = NULL;
		}
		if (node==NULL) {
			fread(ptr,1,8*t16+4,fd);
		} else if (t16>0) {
			fread(ptr,1,8*t16,fd);
		}
		for (t32=0 ; t32<t16 ; t32++) {
			GET64BIT(t64,ptr);
			p->data.fdata.chunktab[t32]=t64;
		}
		p->data.fdata.cuids = NULL;
		if (node==NULL) {	// means loading trash node
			GET32BIT(t32,ptr);
			p->data.fdata.pleng = t32;
			if (t32>0) {
				p->data.fdata.path = malloc(t32);
				if (p->data.fdata.path==NULL) {
					free(p->name);
					free(p);
					return NULL;
				}
				fread(p->data.fdata.path,1,t32,fd);
				if (p->data.fdata.path[0]=='/') {	// trash patch
					p->data.fdata.pleng--;
					memmove(p->data.fdata.path,p->data.fdata.path+1,p->data.fdata.pleng);
				}
			} else {
				p->data.fdata.path = NULL;
			}
		} else {
			p->data.fdata.path = NULL;
			p->data.fdata.pleng = 0;
		}
	}
	if (node!=NULL) {
		p->parent = node;
		node->data.ddata.elements++;
		if (type==TYPE_DIRECTORY) {
			node->data.ddata.nlink++;
		}
		p->next_in_chain = node->data.ddata.children;
		node->data.ddata.children = p;
	} else {
		p->parent = NULL;
		p->next_in_chain = trash;
		trash = p;
		trashspace += p->data.fdata.length;
		trashnodes++;
	}
	nodepos = NODEHASHPOS(p->id);
	p->next_in_idhash = nodehash[nodepos];
	nodehash[nodepos] = p;
	fsnodes_used_inode(p->id);
	nodes++;
	if (type==TYPE_DIRECTORY) {
		dirnodes++;
	}
	if (type==TYPE_FILE) {
		filenodes++;
	}
	return p;
}
*/
#endif

void fs_loadrootnode_1_4(FILE *fd) {
	uint8_t dnodebuff[1+2+4+4+4+4+4+4];
	uint8_t *ptr,t8;
	uint16_t t16;
	fsnode *p;
	uint32_t t32,nodepos;

	p = malloc(sizeof(fsnode));
	root = p;
	fread(dnodebuff,1,1+2+4+4+4+4+4+4,fd);
	ptr = dnodebuff;
	p->id = MFS_ROOT_ID;
	GET8BIT(t8,ptr);
	p->goal = t8;
	GET16BIT(t16,ptr);
	p->mode = t16;
	GET32BIT(t32,ptr);
	p->uid = t32;
	GET32BIT(t32,ptr);
	p->gid = t32;
	GET32BIT(t32,ptr);
	p->atime = t32;
	GET32BIT(t32,ptr);
	p->mtime = t32;
	GET32BIT(t32,ptr);
	p->ctime = t32;
	GET32BIT(t32,ptr);
	p->trashtime = t32;
	p->type = TYPE_DIRECTORY;
	p->data.ddata.children = NULL;
	p->data.ddata.elements = 0;
	p->data.ddata.nlink = 2;
	p->parents = NULL;
	nodepos = NODEHASHPOS(p->id);
	p->next = nodehash[nodepos];
	nodehash[nodepos] = p;
	fsnodes_used_inode(p->id);
	nodes=1;
	dirnodes=1;
	filenodes=0;
}

#ifndef METARESTORE
/*
void fs_loadrootnode_1_23(FILE *fd) {
	uint8_t dnodebuff[1+2+4+4+4+4+4];
	uint8_t *ptr,t8;
	uint16_t t16;
	fsnode *p;
	uint32_t t32,nodepos;

	p = malloc(sizeof(fsnode));
	root = p;
	fread(dnodebuff,1,1+2+4+4+4+4+4,fd);
	ptr = dnodebuff;
	p->id = MFS_ROOT_ID;
	p->nleng = 0;
	p->name = NULL;
	GET8BIT(t8,ptr);
	p->goal = t8&0x0F;
	p->trashtime = (t8&0x10)?0:DEFAULT_TRASHTIME;
	GET16BIT(t16,ptr);
	p->mode = t16;
	GET32BIT(t32,ptr);
	p->uid = t32;
	GET32BIT(t32,ptr);
	p->gid = t32;
	GET32BIT(t32,ptr);
	p->atime = t32;
	GET32BIT(t32,ptr);
	p->mtime = t32;
	GET32BIT(t32,ptr);
	p->ctime = t32;
	p->type = TYPE_DIRECTORY;
	p->data.ddata.children = NULL;
	p->data.ddata.elements = 0;
	p->data.ddata.nlink = 2;
	p->parent = p;
	p->next_in_chain = NULL;
	nodepos = NODEHASHPOS(p->id);
	p->next_in_idhash = nodehash[nodepos];
	nodehash[nodepos] = p;
	fsnodes_used_inode(p->id);
	nodes=1;
	dirnodes=1;
	filenodes=0;
}
*/
#endif

/*
void fs_storenode(fsnode *f,FILE *fd) {
	fsnode *ptr;
	if (f==NULL) {
		fs_storerootnode(fd);
		for (ptr=root->data.ddata.children ; ptr ; ptr=ptr->next_in_chain) {
			fs_storenode(ptr,fd);
		}
		fputc('e',fd);
	} else {
		fputc(f->type,fd);
		fs_storeuninode(FLAG_TREE,f,fd);
		if (f->type==TYPE_DIRECTORY) {
			for (ptr=f->data.ddata.children ; ptr ; ptr=ptr->next_in_chain) {
				fs_storenode(ptr,fd);
			}
			fputc('e',fd);
		}
	}
}
*/

int fs_loadnode_1_4(fsnode *f,FILE *fd) {
	fsnode *d;
	uint8_t type;
	if (f==NULL) {
		fs_loadrootnode_1_4(fd);
		return fs_loadnode_1_4(root,fd);
	} else {
		for (;;) {
			type = fgetc(fd);
			if (type=='e') {
				return 0;
			} else {
				d = fs_loaduninode_1_4(FLAG_TREE,type,f,fd);
				if (!d) {
					return -1;
				}
				if (type==TYPE_DIRECTORY) {
					if (fs_loadnode_1_4(d,fd)<0) {
						return -1;
					}
				}
			}
		}
	}
}

#ifndef METARESTORE
/*
int fs_loadnode_1_3(fsnode *f,FILE *fd) {
	fsnode *d;
	uint8_t type;
	if (f==NULL) {
		fs_loadrootnode_1_23(fd);
		return fs_loadnode_1_3(root,fd);
	} else {
		for (;;) {
			type = fgetc(fd);
			if (type=='e') {
				return 0;
			} else {
				d = fs_loaduninode_1_3(type,f,fd);
				if (!d) {
					return -1;
				}
				if (type==TYPE_DIRECTORY) {
					if (fs_loadnode_1_3(d,fd)<0) {
						return -1;
					}
				}
			}
		}
	}
}

int fs_loadnode_1_2(fsnode *f,FILE *fd) {
	fsnode *d;
	uint8_t type;
	if (f==NULL) {
		fs_loadrootnode_1_23(fd);
		return fs_loadnode_1_2(root,fd);
	} else {
		for (;;) {
			type = fgetc(fd);
			if (type=='e') {
				return 0;
			} else {
				d = fs_loaduninode_1_2(type,f,fd);
				if (!d) {
					return -1;
				}
				if (type==TYPE_DIRECTORY) {
					if (fs_loadnode_1_2(d,fd)<0) {
						return -1;
					}
				}
			}
		}
	}
}
*/
#endif
/*
void fs_storetrash(FILE *fd) {
	fsnode *f;
	for (f=trash ; f ; f=f->next_in_chain) {
		if (f->type==TYPE_FILE) {
			fputc('t',fd);
			fs_storeuninode(FLAG_TRASH,f,fd);
		} else {
			syslog(LOG_WARNING,"structure error: object of type '%c' found in trash !!!",f->type);
		}
	}
	fputc('e',fd);
}
*/

int fs_loadtrash_1_4(FILE *fd) {
	uint8_t type;
	for (;;) {
		type = fgetc(fd);
		if (type=='e') {
			return 0;
		} else if (type=='t') {
			if (fs_loaduninode_1_4(FLAG_TRASH,'f',NULL,fd)==NULL) {
				return -1;
			}
		} else {
			return -1;
		}
	}
}

#ifndef METARESTORE
/*
int fs_loadtrash_1_3(FILE *fd) {
	uint8_t type;
	for (;;) {
		type = fgetc(fd);
		if (type=='e') {
			return 0;
		} else if (type=='t') {
			if (fs_loaduninode_1_3('f',NULL,fd)==NULL) {
				return -1;
			}
		} else {
			return -1;
		}
	}
}

int fs_loadtrash_1_2(FILE *fd) {
	uint8_t type;
	for (;;) {
		type = fgetc(fd);
		if (type=='e') {
			return 0;
		} else if (type=='t') {
			if (fs_loaduninode_1_2('f',NULL,fd)==NULL) {
				return -1;
			}
		} else {
			return -1;
		}
	}
}
*/
#endif

/*
void fs_storereserved(FILE *fd) {
	fsnode *f;
	for (f=reserved ; f ; f=f->next_in_chain) {
		if (f->type==TYPE_FILE) {
			fputc('r',fd);
			fs_storeuninode(FLAG_RESERVED,f,fd);
		} else {
			syslog(LOG_WARNING,"structure error: object of type '%c' found as opened file !!!",f->type);
		}
	}
	fputc('e',fd);
}
*/

int fs_loadreserved_1_4(FILE *fd) {
	uint8_t type;
	for (;;) {
		type = fgetc(fd);
		if (type=='e') {
			return 0;
		} else if (type=='r') {
			if (fs_loaduninode_1_4(FLAG_RESERVED,'f',NULL,fd)==NULL) {
				return -1;
			}
		} else {
			return -1;
		}
	}
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
	PUT32BIT(l,ptr);
	fwrite(wbuff,1,4,fd);
	l=0;
	ptr=wbuff;
	for (n=freelist ; n ; n=n->next) {
		if (l==1024) {
			fwrite(wbuff,1,8*1024,fd);
			l=0;
			ptr=wbuff;
		}
		PUT32BIT(n->id,ptr);
		PUT32BIT(n->ftime,ptr);
		l++;
	}
	if (l>0) {
		fwrite(wbuff,1,8*l,fd);
	}
}

int fs_loadfree(FILE *fd) {
	uint8_t rbuff[8*1024],*ptr;
	freenode *n;
	uint32_t l,t;
	if (fread(rbuff,1,4,fd)!=4) {
		return -1;
	}
	ptr=rbuff;
	GET32BIT(t,ptr);
	freelist = NULL;
	freetail = &(freelist);
	l=0;
	while (t>0) {
		if (l==0) {
			if (t>1024) {
				if (fread(rbuff,1,8*1024,fd)!=8*1024) {
					return -1;
				}
				l=1024;
			} else {
				if (fread(rbuff,1,8*t,fd)!=8*t) {
					return -1;
				}
				l=t;
			}
			ptr = rbuff;
		}
		n = (freenode*)malloc(sizeof(freenode));
		if (n==NULL) {
			return -1;
		}
		GET32BIT(n->id,ptr);
		GET32BIT(n->ftime,ptr);
		n->next = NULL;
		*freetail = n;
		freetail = &(n->next);
		fsnodes_used_inode(n->id);
		l--;
		t--;
	}
	return 0;
}

void fs_store(FILE *fd) {
	uint8_t hdr[16];
	uint8_t *ptr;
	ptr = hdr;
	PUT32BIT(maxnodeid,ptr);
	PUT64BIT(version,ptr);
	PUT32BIT(nextcuid,ptr);
	fwrite(hdr,1,16,fd);
	fs_storenodes(fd);
	fs_storeedges(fd);
	fs_storefree(fd);
}

int fs_load(FILE *fd) {
	uint8_t hdr[16];
	uint8_t *ptr;
	if (fread(hdr,1,16,fd)!=16) {
		return -1;
	}
	ptr = hdr;
	GET32BIT(maxnodeid,ptr);
	GET64BIT(version,ptr);
	GET32BIT(nextcuid,ptr);
	fsnodes_init_freebitmask();
	if (fs_loadnodes(fd)<0) {
#ifdef METARESTORE
		printf("error reading metadata (node)\n");
#else
		syslog(LOG_ERR,"error reading metadata (node)");
#endif
		return -1;
	}
	if (fs_loadedges(fd)<0) {
#ifdef METARESTORE
		printf("error reading metadata (edge)\n");
#else
		syslog(LOG_ERR,"error reading metadata (edge)");
#endif
		return -1;
	}
	if (fs_loadfree(fd)<0) {
#ifdef METARESTORE
		printf("error reading metadata (free)\n");
#else
		syslog(LOG_ERR,"error reading metadata (free)");
#endif
		return -1;
	}
	root = fsnodes_id_to_node(MFS_ROOT_ID);
	if (root==NULL) {
#ifdef METARESTORE
		printf("error reading metadata (no root)\n");
#else
		syslog(LOG_ERR,"error reading metadata (no root)");
#endif
		return -1;
	}
	if (fs_checknodes()<0) {
		return -1;
	}
	return 0;
}


int fs_load_1_4(FILE *fd) {
	uint8_t hdr[16];
	uint8_t *ptr;
	fread(hdr,1,16,fd);
	ptr = hdr;
	GET32BIT(maxnodeid,ptr);
	GET64BIT(version,ptr);
	GET32BIT(nextcuid,ptr);
	fsnodes_init_freebitmask();
	if (fs_loadnode_1_4(NULL,fd)<0 || root==NULL) {
#ifdef METARESTORE
		printf("error reading metadata (tree)\n");
#else
		syslog(LOG_ERR,"error reading metadata (tree)");
#endif
		return -1;
	}
	if (fs_loadtrash_1_4(fd)<0) {
#ifdef METARESTORE
		printf("error reading metadata (trash)\n");
#else
		syslog(LOG_ERR,"error reading metadata (trash)");
#endif
		return -1;
	}
	if (fs_loadreserved_1_4(fd)<0) {
#ifdef METARESTORE
		printf("error reading metadata (reserved)\n");
#else
		syslog(LOG_ERR,"error reading metadata (reserved)");
#endif
		return -1;
	}
	if (fs_loadfree(fd)<0) {
#ifdef METARESTORE
		printf("error reading metadata (free)\n");
#else
		syslog(LOG_ERR,"error reading metadata (free)");
#endif
		return -1;
	}
	return 0;
}

#ifndef METARESTORE
/*
int fs_load_1_2(FILE *fd) {
	uint8_t hdr[8];
	uint8_t *ptr;
	uint32_t v32;
	fread(hdr,1,8,fd);
	ptr = hdr;
	GET32BIT(maxnodeid,ptr);
	GET32BIT(v32,ptr);
	version = v32;
	nextcuid = 1;
	fsnodes_init_freebitmask();
	if (fs_loadnode_1_2(NULL,fd)<0 || root==NULL) {
		return -1;
	}
	freelist = NULL;
	freetail = &(freelist);
	return fs_loadtrash_1_2(fd);
}

int fs_load_1_3(FILE *fd) {
	uint8_t hdr[8];
	uint8_t *ptr;
	uint32_t v32;
	fread(hdr,1,8,fd);
	ptr = hdr;
	GET32BIT(maxnodeid,ptr);
	GET32BIT(v32,ptr);
	version = v32;
	nextcuid = 1;
	fsnodes_init_freebitmask();
	if (fs_loadnode_1_3(NULL,fd)<0 || root==NULL) {
		return -1;
	}
	freelist = NULL;
	freetail = &(freelist);
	return fs_loadtrash_1_3(fd);
}
*/
void fs_new(void) {
	uint32_t nodepos;
	maxnodeid = MFS_ROOT_ID;
	version = 0;
	nextcuid = 1;
	fsnodes_init_freebitmask();
	root = malloc(sizeof(fsnode));
	root->id = MFS_ROOT_ID;
	root->type = TYPE_DIRECTORY;
	root->ctime = root->mtime = root->atime = main_time();
	root->goal = DEFAULT_GOAL;
	root->trashtime = DEFAULT_TRASHTIME;
	root->mode = 0777;
	root->uid = 0;
	root->gid = 0;
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
}
#endif

#ifndef METARESTORE
int fs_storeall(int bg) {
	FILE *fd;
#ifdef BACKGROUND_METASTORE
	int i;
	struct stat sb;
	if (stat("metadata.mfs.back.tmp",&sb)==0) {
		return -1;
	}
#else
	(void)bg;
#endif
	rotatelog();
#ifdef BACKGROUND_METASTORE
	if (bg) {
		i = fork();
	} else {
		i = -1;
	}
	// if fork returned -1 (fork error) store metadata in foreground !!!
	if (i<=0) {
#endif
		if (rename("metadata.mfs.back","metadata.mfs.back.tmp")<0) {
			syslog(LOG_ERR,"can't rename metadata.mfs.back -> metadata.mfs.back.tmp (%m)");
			return 0;
		}
		fd = fopen("metadata.mfs.back","w");
		if (fd==NULL) {
			syslog(LOG_ERR,"can't open metadata file");
#ifdef BACKGROUND_METASTORE
			if (i==0) {
				exit(0);
			}
#endif
			return 0;
		}
		fwrite("MFSM 1.5",1,8,fd);
		fs_store(fd);
		chunk_store(fd);
		if (ferror(fd)!=0) {
			syslog(LOG_ERR,"can't write metadata");
		}
		fclose(fd);
		unlink("metadata.mfs.back.tmp");
#ifdef BACKGROUND_METASTORE
		if (i==0) {
			exit(0);
		}
	}
#endif
	return 1;
}

void fs_dostoreall(void) {
	fs_storeall(1);	// ignore error
}

void fs_term(void) {
	int u;
	for (u=0 ; u<3 ; u++) {
		if (fs_storeall(0)==1) {
			if (rename("metadata.mfs.back","metadata.mfs")<0) {
				syslog(LOG_WARNING,"can't rename metadata.mfs.back -> metadata.mfs (%m)");
			}
			return ;
		}
		sleep(5);
	}
}

#else
void fs_storeall(const char *fname) {
	FILE *fd;
	fd = fopen(fname,"w");
	if (fd==NULL) {
		printf("can't open metadata file\n");
		return;
	}
	fwrite("MFSM 1.5",1,8,fd);
	fs_store(fd);
	chunk_store(fd);
	if (ferror(fd)!=0) {
		printf("can't write metadata\n");
	}
	fclose(fd);
}

void fs_term(const char *fname) {
	fs_storeall(fname);
}
#endif

#ifndef METARESTORE
int fs_loadall(void) {
#else
int fs_loadall(const char *fname) {
#endif
	FILE *fd;
	uint8_t hdr[8];
#ifndef METARESTORE
	int converted=0;
#endif

#ifdef METARESTORE
	fd = fopen(fname,"r");
#else
	fd = fopen("metadata.mfs","r");
#endif
	if (fd==NULL) {
#ifdef METARESTORE
		printf("can't open metadata file\n");
#else
		syslog(LOG_ERR,"can't open metadata file");
#endif
		return -1;
	}
	fread(hdr,1,8,fd);
#ifndef METARESTORE
	if (memcmp(hdr,"MFSM NEW",8)==0) {	// special case - create new file system
		fclose(fd);
		if (rename("metadata.mfs","metadata.mfs.back")<0) {
			syslog(LOG_ERR,"can't rename metadata.mfs -> metadata.mfs.back (%m)");
			return -1;
		}
		syslog(LOG_NOTICE,"create new empty filesystem");
		fs_new();
		return 0;
	}
	if (memcmp(hdr,"MFSM 1.4",8)==0) {
		converted=1;
		if (fs_load_1_4(fd)<0) {
			syslog(LOG_ERR,"error reading metadata (structure)");
			fclose(fd);
			return -1;
		}
		if (chunk_load(fd)<0) {
			syslog(LOG_ERR,"error reading metadata (chunks)");
			fclose(fd);
			return -1;
		}
	} else 
#endif
	if (memcmp(hdr,"MFSM 1.5",8)==0) {
		if (fs_load(fd)<0) {
#ifdef METARESTORE
			printf("error reading metadata (structure)\n");
#else
			syslog(LOG_ERR,"error reading metadata (structure)");
#endif
			fclose(fd);
			return -1;
		}
		if (chunk_load(fd)<0) {
#ifdef METARESTORE
			printf("error reading metadata (chunks)\n");
#else
			syslog(LOG_ERR,"error reading metadata (chunks)");
#endif
			fclose(fd);
			return -1;
		}
	} else {
#ifdef METARESTORE
		printf("wrong metadata header (old version ?)\n");
#else
		syslog(LOG_ERR,"wrong metadata header");
#endif
		fclose(fd);
		return -1;
	}
	if (ferror(fd)!=0) {
#ifdef METARESTORE
		printf("error reading metadata\n");
#else
		syslog(LOG_ERR,"error reading metadata");
#endif
		fclose(fd);
		return -1;
	}
	fclose(fd);
#ifndef METARESTORE
	if (converted==1) {
		if (rename("metadata.mfs","metadata.mfs.back.1.4")<0) {
			syslog(LOG_ERR,"can't rename metadata.mfs -> metadata.mfs.back.1.4 (%m)");
			return -1;
		}
		fs_storeall(0);	// after conversion always create new version of "back" file for using in proper version of metarestore
//	} else if (converted==2) {
//		rename("metadata.mfs","metadata.mfs.back.1.3");
//		fs_storeall(0);	// after conversion always create new version of "back" file for using in proper version of metarestore
	} else {
		if (rename("metadata.mfs","metadata.mfs.back")<0) {
			syslog(LOG_ERR,"can't rename metadata.mfs -> metadata.mfs.back (%m)");
			return -1;
		}
	}
#endif
	fs_add_files_to_chunks();
	return 0;
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
int fs_init(void) {
	fs_strinit();
	chunk_strinit();
	starttime = main_time();
	if (fs_loadall()<0) {
		return -1;
	}
	main_timeregister(1,0,fs_test_files);
	main_timeregister(3600,0,fs_dostoreall);
	main_timeregister(300,0,fs_emptytrash);
	main_timeregister(60,0,fs_emptyreserved);
	main_timeregister(60,0,fs_show_counts);
	main_timeregister(60,0,fsnodes_freeinodes);
	main_destructregister(fs_term);
	return 0;
}
#else
int fs_init(const char *fname) {
	fs_strinit();
	chunk_strinit();
	if (fs_loadall(fname)<0) {
		return -1;
	}
	return 0;
}
#endif
