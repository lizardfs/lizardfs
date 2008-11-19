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

#include "config.h"

#include <time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <inttypes.h>
#include <netinet/in.h>

#include "MFSCommunication.h"

#include "datapack.h"
#include "matocuserv.h"
#include "matocsserv.h"
#include "chunks.h"
#include "filesystem.h"
#include "stats.h"
#include "cfg.h"
#include "main.h"
#include "sockets.h"

#define MaxPacketSize 1000000

// matocuserventry.mode
enum {KILL,HEADER,DATA};
// chunklis.type
enum {FUSE_WRITE,FUSE_SETATTR,FUSE_TRUNCATE};

#define CURECORD_TIMEOUT 7200

// locked chunks
typedef struct chunklist {
	uint64_t chunkid;
	uint64_t fleng;		// file length
	uint32_t qid;		// queryid for answer
	uint32_t inode;		// inode
	uint8_t type;
	struct chunklist *next;
} chunklist;

// opened files
typedef struct filelist {
	uint32_t inode;
	struct filelist *next;
} filelist;

typedef struct custrecord {
	uint32_t cuid;
	uint32_t disconnected;	// 0 = connected ; other = disconnection timestamp
	uint32_t nsocks;	// >0 - connected (number of active connections) ; 0 - not connected
	uint32_t currentopstats[16];
	uint32_t lasthouropstats[16];
	filelist *openedfiles;
	struct custrecord *next;
} custrecord;

typedef struct packetstruct {
	struct packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t *packet;
} packetstruct;

typedef struct matocuserventry {
	uint8_t registered;
	uint8_t mode;				//0 - not active, 1 - read header, 2 - read packet
	int sock;				//socket number
	uint32_t lastread,lastwrite;		//time of last activity
	uint32_t version;
	uint8_t hdrbuff[8];
	packetstruct inputpacket;
	packetstruct *outputhead,**outputtail;

	custrecord *curec;
	chunklist *chunkdelayedops;
//	filelist *openedfiles;

	struct matocuserventry *next;
} matocuserventry;

static custrecord *curechead=NULL;
static matocuserventry *matocuservhead=NULL;
static int lsock;
static int exiting;

// from config
static char *ListenHost;
static char *ListenPort;
//static uint32_t Timeout;


custrecord* matocuserv_get_customer(uint32_t cuid) {
	// if cuid==0 - create new record with next id
	custrecord *acurec;

	if (cuid>0) {
		for (acurec = curechead ; acurec ; acurec=acurec->next) {
			if (acurec->cuid==cuid) {
				acurec->nsocks++;
				acurec->disconnected = 0;
				return acurec;
			}
		}
	}
	acurec = (custrecord*)malloc(sizeof(custrecord));
	if (cuid==0) {
		acurec->cuid = fs_newcuid();
	} else {
		acurec->cuid = cuid;
	}
	acurec->openedfiles = NULL;
	acurec->disconnected = 0;
	acurec->nsocks = 1;
	memset(acurec->currentopstats,0,4*16);
	memset(acurec->lasthouropstats,0,4*16);
	acurec->next = curechead;
	curechead = acurec;
	return acurec;
}

int matocuserv_insert_openfile(custrecord* cr,uint32_t inode) {
	filelist *ofptr,**ofpptr;
	int status;

	ofpptr = &(cr->openedfiles);
	while ((ofptr=*ofpptr)) {
		if (ofptr->inode==inode) {
			return STATUS_OK;	// file already aquired - nothing to do
		}
		if (ofptr->inode>inode) {
			break;
		}
		ofpptr = &(ofptr->next);
	}
	status = fs_aquire(inode,cr->cuid);
	if (status==STATUS_OK) {
		ofptr = (filelist*)malloc(sizeof(filelist));
		ofptr->inode = inode;
		ofptr->next = *ofpptr;
		*ofpptr = ofptr;
	}
	return status;
}

void matocuserv_init_customers(uint32_t cuid,uint32_t inode) {
	custrecord *acurec;
	filelist *ofptr,**ofpptr;

	for (acurec = curechead ; acurec && acurec->cuid!=cuid; acurec=acurec->next) ;
	if (acurec==NULL) {
		acurec = (custrecord*)malloc(sizeof(custrecord));
		acurec->cuid = cuid;
		acurec->openedfiles = NULL;
		acurec->disconnected = main_time();
		acurec->nsocks = 0;
		memset(acurec->currentopstats,0,4*16);
		memset(acurec->lasthouropstats,0,4*16);
		acurec->next = curechead;
		curechead = acurec;
	}

	ofpptr = &(acurec->openedfiles);
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
	ofptr->inode = inode;
	ofptr->next = *ofpptr;
	*ofpptr = ofptr;
}

uint8_t* matocuserv_createpacket(matocuserventry *eptr,uint32_t type,uint32_t size) {
	packetstruct *outpacket;
	uint8_t *ptr;
	uint32_t psize;

	outpacket=(packetstruct*)malloc(sizeof(packetstruct));
	if (outpacket==NULL) {
		return NULL;
	}
	psize = size+8;
	outpacket->packet=malloc(psize);
	outpacket->bytesleft = psize;
	if (outpacket->packet==NULL) {
		free(outpacket);
		return NULL;
	}
	ptr = outpacket->packet;
	PUT32BIT(type,ptr);
	PUT32BIT(size,ptr);
	outpacket->startptr = (uint8_t*)(outpacket->packet);
	outpacket->next = NULL;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
	return ptr;
}

/*
int matocuserv_open_check(matocuserventry *eptr,uint32_t fid) {
	filelist *fl;
	for (fl=eptr->openedfiles ; fl ; fl=fl->next) {
		if (fl->fid==fid) {
			return 0;
		}
	}
	return -1;
}
*/

void matocuserv_chunk_status(uint64_t chunkid,uint8_t status) {
	uint32_t qid,inode;
	uint64_t fleng;
	uint8_t type,attr[35];
	uint32_t version;
//	uint8_t rstat;
	uint32_t ip;
	uint16_t port;
	uint8_t *ptr;
	uint8_t i,count;
	void *sptr[256];
	chunklist *cl,**acl;
	matocuserventry *eptr,*eaptr;

	eptr=NULL;
	qid=0;
	fleng=0;
	type=0;
	inode=0;
	for (eaptr = matocuservhead ; eaptr && eptr==NULL ; eaptr=eaptr->next) {
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
	switch (type) {
	case FUSE_WRITE:
		if (status==STATUS_OK) {
			status=chunk_getversionandlocations(chunkid,&version,&count,sptr);
			//syslog(LOG_NOTICE,"get version for chunk %llu -> %lu",chunkid,version);
		}
		if (status!=STATUS_OK) {
			ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_WRITE_CHUNK,5);
			if (ptr==NULL) {
				syslog(LOG_NOTICE,"can't allocate memory for packet");
				eptr->mode = KILL;
				return;
			}
			PUT32BIT(qid,ptr);
			PUT8BIT(status,ptr);
			fs_writeend(0,0,chunkid);	// ignore status - just do it.
			return;
		}
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_WRITE_CHUNK,24+count*6);
		if (ptr==NULL) {
			syslog(LOG_NOTICE,"can't allocate memory for packet");
			eptr->mode = KILL;
			return;
		}
		PUT32BIT(qid,ptr);
		PUT64BIT(fleng,ptr);
		PUT64BIT(chunkid,ptr);
		PUT32BIT(version,ptr);
		for (i=0 ; i<count ; i++) {
			if (matocsserv_getlocation(sptr[i],&ip,&port)<0) {
				PUT32BIT(0,ptr);
				PUT16BIT(0,ptr);
			} else {
				PUT32BIT(ip,ptr);
				PUT16BIT(port,ptr);
			}
		}
		return;
	case FUSE_SETATTR:
	case FUSE_TRUNCATE:
		fs_end_setlength(chunkid);

		if (status!=STATUS_OK) {
			ptr = matocuserv_createpacket(eptr,(type==FUSE_SETATTR)?MATOCU_FUSE_SETATTR:MATOCU_FUSE_TRUNCATE,5);
			if (ptr==NULL) {
				syslog(LOG_NOTICE,"can't allocate memory for packet");
				eptr->mode = KILL;
				return;
			}
			PUT32BIT(qid,ptr);
			PUT8BIT(status,ptr);
			return;
		}
		fs_do_setlength(inode,fleng,attr);
		if (eptr->version<0x010500) {
			ptr = matocuserv_createpacket(eptr,(type==FUSE_SETATTR)?MATOCU_FUSE_SETATTR:MATOCU_FUSE_TRUNCATE,36);
		} else {
			ptr = matocuserv_createpacket(eptr,(type==FUSE_SETATTR)?MATOCU_FUSE_SETATTR:MATOCU_FUSE_TRUNCATE,39);
		}
		if (ptr==NULL) {
			syslog(LOG_NOTICE,"can't allocate memory for packet");
			eptr->mode = KILL;
			return;
		}
		PUT32BIT(qid,ptr);
		if (eptr->version<0x010500) {
			fs_attr_to_attr32(attr,ptr);
		} else {
			memcpy(ptr,attr,35);
		}
		return;
	default:
		syslog(LOG_WARNING,"got chunk status, but operation type is unknown");
	}
}

void matocuserv_cserv_list(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CUTOMA_CSERV_LIST - wrong size (%d/0)",length);
		eptr->mode = KILL;
		return;
	}
	ptr = matocuserv_createpacket(eptr,MATOCU_CSERV_LIST,matocsserv_cservlist_size());
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	matocsserv_cservlist_data(ptr);
}

void matocuserv_cust_list(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	matocuserventry *eaptr;
	uint32_t size,ip,i;
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CUTOMA_CUST_LIST - wrong size (%d/0)",length);
		eptr->mode = KILL;
		return;
	}
	size = 0;
	for (eaptr = matocuservhead ; eaptr ; eaptr=eaptr->next) {
		if (eaptr->mode!=KILL && eaptr->registered>0 && eaptr!=eptr) {
			size+=136;
		}
	}
	ptr = matocuserv_createpacket(eptr,MATOCU_CUST_LIST,size);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	for (eaptr = matocuservhead ; eaptr ; eaptr=eaptr->next) {
		if (eaptr->mode!=KILL && eaptr->registered>0 && eaptr!=eptr) {
			tcpgetpeer(eaptr->sock,&ip,NULL);
			PUT32BIT(ip,ptr);
			PUT32BIT(eaptr->version,ptr);
			if (eaptr->curec) {
				for (i=0 ; i<16 ; i++) {
					PUT32BIT(eaptr->curec->currentopstats[i],ptr);
				}
				for (i=0 ; i<16 ; i++) {
					PUT32BIT(eaptr->curec->lasthouropstats[i],ptr);
				}
			} else {
				memset(ptr,0xFF,8*16);
				ptr+=8*16;
			}
		}
	}
}

void matocuserv_chart(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t chartid;
	uint8_t *ptr;
	uint32_t l;

	if (length!=4) {
		syslog(LOG_NOTICE,"CUTOAN_CHART - wrong size (%d/4)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(chartid,data);
	l = stats_gifsize(chartid);
	ptr = matocuserv_createpacket(eptr,ANTOCU_CHART,l);
	if (ptr==NULL) {
		eptr->mode=KILL;
		return;
	}
	if (l>0) {
		stats_makegif(ptr);
	}
}

void matocuserv_chart_data(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t chartid;
	uint8_t *ptr;
	uint32_t l;

	if (length!=4) {
		syslog(LOG_NOTICE,"CUTOAN_CHART_DATA - wrong size (%d/4)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(chartid,data);
	l = stats_datasize(chartid);
	ptr = matocuserv_createpacket(eptr,ANTOCU_CHART_DATA,l);
	if (ptr==NULL) {
		eptr->mode=KILL;
		return;
	}
	if (l>0) {
		stats_makedata(ptr,chartid);
	}
}

void matocuserv_info(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint64_t totalspace,availspace,trspace,respace;
	uint32_t trnodes,renodes,inodes,dnodes,fnodes,chunks,tdchunks;
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CUTOMA_INFO - wrong size (%d/0)",length);
		eptr->mode = KILL;
		return;
	}
	fs_info(&totalspace,&availspace,&trspace,&trnodes,&respace,&renodes,&inodes,&dnodes,&fnodes,&chunks,&tdchunks);
	ptr = matocuserv_createpacket(eptr,MATOCU_INFO,60);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT64BIT(totalspace,ptr);
	PUT64BIT(availspace,ptr);
	PUT64BIT(trspace,ptr);
	PUT32BIT(trnodes,ptr);
	PUT64BIT(respace,ptr);
	PUT32BIT(renodes,ptr);
	PUT32BIT(inodes,ptr);
	PUT32BIT(dnodes,ptr);
	PUT32BIT(fnodes,ptr);
	PUT32BIT(chunks,ptr);
	PUT32BIT(tdchunks,ptr);
}

void matocuserv_fstest_info(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t loopstart,loopend,files,ugfiles,mfiles,chunks,ugchunks,mchunks,msgbuffleng;
	char *msgbuff;
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CUTOMA_FSTEST_INFO - wrong size (%d/0)",length);
		eptr->mode = KILL;
		return;
	}
	fs_test_getdata(&loopstart,&loopend,&files,&ugfiles,&mfiles,&chunks,&ugchunks,&mchunks,&msgbuff,&msgbuffleng);
	ptr = matocuserv_createpacket(eptr,MATOCU_FSTEST_INFO,msgbuffleng+36);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(loopstart,ptr);
	PUT32BIT(loopend,ptr);
	PUT32BIT(files,ptr);
	PUT32BIT(ugfiles,ptr);
	PUT32BIT(mfiles,ptr);
	PUT32BIT(chunks,ptr);
	PUT32BIT(ugchunks,ptr);
	PUT32BIT(mchunks,ptr);
	PUT32BIT(msgbuffleng,ptr);
	if (msgbuffleng>0) {
		memcpy(ptr,msgbuff,msgbuffleng);
	}
}

void matocuserv_chunkstest_info(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CUTOMA_CHUNKSTEST_INFO - wrong size (%d/0)",length);
		eptr->mode = KILL;
		return;
	}
	ptr = matocuserv_createpacket(eptr,MATOCU_CHUNKSTEST_INFO,52);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	chunk_store_info(ptr);
}

void matocuserv_fuse_register(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint32_t cuid;

	if (length!=64 && length!=68 && length!=72) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_REGISTER - wrong size (%d/64|68|72)",length);
		eptr->mode = KILL;
		return;
	}
	if (memcmp(data,FUSE_REGISTER_BLOB,64)==0 && length==64) {
		eptr->registered=1;
		eptr->version = 1;
	} else if (memcmp(data,FUSE_REGISTER_BLOB_NOPS,64)==0 && length==64) {
		eptr->registered=2;
		eptr->version = 2;
	} else if (memcmp(data,FUSE_REGISTER_BLOB_DNAMES,64)==0 && (length==68 || length==72)) {
		eptr->registered=3;
		eptr->version = 3;
	} else {
		eptr->registered=0;
	}
	if (eptr->registered>=3) {
		ptr = data+64;
		GET32BIT(cuid,ptr);
		if (length==72) {
			GET32BIT(eptr->version,ptr);
		}
		eptr->curec = matocuserv_get_customer(cuid);
		if (eptr->curec==NULL) {
			syslog(LOG_NOTICE,"can't allocate customer record");
			eptr->mode = KILL;
			return;
		}
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_REGISTER,4);
		if (ptr==NULL) {
			syslog(LOG_NOTICE,"can't allocate memory for packet");
			eptr->mode = KILL;
			return;
		}
		cuid = eptr->curec->cuid;
		PUT32BIT(cuid,ptr);
	} else {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_REGISTER,1);
		if (ptr==NULL) {
			syslog(LOG_NOTICE,"can't allocate memory for packet");
			eptr->mode = KILL;
			return;
		}
		eptr->curec = matocuserv_get_customer(0);
		if (eptr->registered==0) {
			PUT8BIT(ERROR_EACCES,ptr);
		} else {
			PUT8BIT(STATUS_OK,ptr);
		}
	}
}

void matocuserv_fuse_reserved_inodes(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	filelist *ofptr,**ofpptr;
	uint32_t inode;

	if (eptr->registered<3) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_RESERVED_INODES - command unavailable for this customer");
		eptr->mode = KILL;
		return;
	}
	if ((length&0x3)!=0) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_RESERVED_INODES - wrong size (%d/N*4)",length);
		eptr->mode = KILL;
		return;
	}

	ptr = data;
//	endptr = ptr + length;
	ofpptr = &(eptr->curec->openedfiles);
	length >>= 2;
	if (length) {
		length--;
		GET32BIT(inode,ptr);
	} else {
		inode=0;
	}

	while ((ofptr=*ofpptr) && inode>0) {
		if (ofptr->inode<inode) {
			fs_release(ofptr->inode,eptr->curec->cuid);
			*ofpptr = ofptr->next;
			free(ofptr);
		} else if (ofptr->inode>inode) {
			if (fs_aquire(inode,eptr->curec->cuid)==STATUS_OK) {
				ofptr = (filelist*)malloc(sizeof(filelist));
				ofptr->next = *ofpptr;
				ofptr->inode = inode;
				*ofpptr = ofptr;
				ofpptr = &(ofptr->next);
			}
			if (length) {
				length--;
				GET32BIT(inode,ptr);
			} else {
				inode=0;
			}
		} else {
			ofpptr = &(ofptr->next);
			if (length) {
				length--;
				GET32BIT(inode,ptr);
			} else {
				inode=0;
			}
		}
	}
	while (inode>0) {
		if (fs_aquire(inode,eptr->curec->cuid)==STATUS_OK) {
			ofptr = (filelist*)malloc(sizeof(filelist));
			ofptr->next = *ofpptr;
			ofptr->inode = inode;
			*ofpptr = ofptr;
			ofpptr = &(ofptr->next);
		}
		if (length) {
			length--;
			GET32BIT(inode,ptr);
		} else {
			inode=0;
		}
	}
	while ((ofptr=*ofpptr)) {
		fs_release(ofptr->inode,eptr->curec->cuid);
		*ofpptr = ofptr->next;
		free(ofptr);
	}

}

void matocuserv_fuse_statfs(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint64_t totalspace,availspace,trashspace,reservedspace;
	uint32_t msgid,inodes;
	uint8_t *ptr;
	if (length!=4) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_STATFS - wrong size (%d/4)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	fs_statfs(&totalspace,&availspace,&trashspace,&reservedspace,&inodes);
	if (eptr->registered>=3) {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_STATFS,40);
	} else {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_STATFS,20);
	}
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	PUT64BIT(totalspace,ptr);
	PUT64BIT(availspace,ptr);
	if (eptr->registered>=3) {
		PUT64BIT(trashspace,ptr);
		PUT64BIT(reservedspace,ptr);
		PUT32BIT(inodes,ptr);
	}
	if (eptr->curec) {
		eptr->curec->currentopstats[0]++;
	}
}

void matocuserv_fuse_access(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint8_t modemask;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=17) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_ACCESS - wrong size (%d/17)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	GET32BIT(uid,data);
	GET32BIT(gid,data);
	GET8BIT(modemask,data);
	status = fs_access(inode,uid,gid,modemask);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_ACCESS,5);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	PUT8BIT(status,ptr);
}

void matocuserv_fuse_lookup(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint8_t nleng;
	uint8_t *name;
	uint32_t newinode;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (eptr->registered<3) {
		if (length!=272) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_LOOKUP - wrong size (%d/272)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode,data);
		name = data;
		data+=256;
		GET32BIT(uid,data);
		GET32BIT(gid,data);
		nleng = strlen((char*)name);
	} else {
		if (length<17) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_LOOKUP - wrong size (%d)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode,data);
		GET8BIT(nleng,data);
		if (length!=17U+nleng) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_LOOKUP - wrong size (%d:nleng=%d)",length,nleng);
			eptr->mode = KILL;
			return;
		}
		name = data;
		data += nleng;
		GET32BIT(uid,data);
		GET32BIT(gid,data);
	}
	status = fs_lookup(inode,nleng,name,uid,gid,&newinode,attr);
	if (eptr->version<0x010500) {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_LOOKUP,(status!=STATUS_OK)?5:40);
	} else {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_LOOKUP,(status!=STATUS_OK)?5:43);
	}
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		PUT32BIT(newinode,ptr);
		if (eptr->version<0x010500) {
			fs_attr_to_attr32(attr,ptr);
		} else {
			memcpy(ptr,attr,35);
		}
	}
	if (eptr->curec) {
		eptr->curec->currentopstats[3]++;
	}
}

void matocuserv_fuse_getattr(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_GETATTR - wrong size (%d/8)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	status = fs_getattr(inode,attr);
	if (eptr->version<0x010500) {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_GETATTR,(status!=STATUS_OK)?5:36);
	} else {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_GETATTR,(status!=STATUS_OK)?5:39);
	}
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		if (eptr->version<0x010500) {
			fs_attr_to_attr32(attr,ptr);
		} else {
			memcpy(ptr,attr,35);
		}
	}
	if (eptr->curec) {
		eptr->curec->currentopstats[1]++;
	}
}

void matocuserv_fuse_setattr(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint16_t setmask;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint16_t attrmode;
	uint32_t attruid,attrgid,attratime,attrmtime;
	uint64_t attrlength;
	chunklist *cl;
	uint64_t chunkid;
	if (eptr->version<0x010500) {
		if (length!=49 && length!=50) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_SETATTR - wrong size (%d/49|50)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode,data);
		GET32BIT(uid,data);
		GET32BIT(gid,data);
		if (length==49) {
			GET8BIT(setmask,data);
		} else {
			GET16BIT(setmask,data);
		}
		if (setmask&(SET_GOAL_FLAG|SET_DELETE_FLAG)) {
			status = ERROR_EINVAL;
		} else {
			fs_attr32_to_attrvalues(data,&attrmode,&attruid,&attrgid,&attratime,&attrmtime,&attrlength);
			if (setmask&(SET_MODE_FLAG|SET_UID_FLAG|SET_GID_FLAG|SET_ATIME_FLAG|SET_MTIME_FLAG)) {
				status = fs_setattr(inode,uid,gid,setmask,attrmode,attruid,attrgid,attratime,attrmtime,attr);
			} else {
				status = STATUS_OK;
			}
			if (status==STATUS_OK) {
				if (setmask&SET_LENGTH_FLAG) {
					status = fs_try_setlength(inode,(setmask&SET_OPENED_FLAG)?0:uid,gid,attrlength,attr,&chunkid);
					if (status==ERROR_DELAYED) {
						cl = (chunklist*)malloc(sizeof(chunklist));
						cl->chunkid = chunkid;
						cl->qid = msgid;
						cl->inode = inode;
						cl->fleng = attrlength;
						cl->type = FUSE_SETATTR;
						cl->next = eptr->chunkdelayedops;
						eptr->chunkdelayedops = cl;
						if (eptr->curec) {
							eptr->curec->currentopstats[2]++;
						}
						return;
					}
					if (status==STATUS_OK) {
						status = fs_do_setlength(inode,attrlength,attr);
					}
				}
			}
		}
	} else {
		if (length!=35) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_SETATTR - wrong size (%d/35)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode,data);
		GET32BIT(uid,data);
		GET32BIT(gid,data);
		GET8BIT(setmask,data);
		GET16BIT(attrmode,data);
		GET32BIT(attruid,data);
		GET32BIT(attrgid,data);
		GET32BIT(attratime,data);
		GET32BIT(attrmtime,data);
		if (setmask&(SET_GOAL_FLAG|SET_LENGTH_FLAG|SET_OPENED_FLAG)) {
			status = ERROR_EINVAL;
		} else {
			status = fs_setattr(inode,uid,gid,setmask,attrmode,attruid,attrgid,attratime,attrmtime,attr);
		}
	}
	if (eptr->version<0x010500) {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_SETATTR,(status!=STATUS_OK)?5:36);
	} else {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_SETATTR,(status!=STATUS_OK)?5:39);
	}
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		if (eptr->version<0x010500) {
			fs_attr_to_attr32(attr,ptr);
		} else {
			memcpy(ptr,attr,35);
		}
	}
	if (eptr->curec) {
		eptr->curec->currentopstats[2]++;
	}
}

void matocuserv_fuse_truncate(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint64_t attrlength;
	chunklist *cl;
	uint64_t chunkid;
	if (length!=24) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_TRUNCATE - wrong size (%d/24)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	GET32BIT(uid,data);
	GET32BIT(gid,data);
	GET64BIT(attrlength,data);
	status = fs_try_setlength(inode,uid,gid,attrlength,attr,&chunkid);
	if (status==ERROR_DELAYED) {
		cl = (chunklist*)malloc(sizeof(chunklist));
		cl->chunkid = chunkid;
		cl->qid = msgid;
		cl->inode = inode;
		cl->fleng = attrlength;
		cl->type = FUSE_TRUNCATE;
		cl->next = eptr->chunkdelayedops;
		eptr->chunkdelayedops = cl;
		if (eptr->curec) {
			eptr->curec->currentopstats[2]++;
		}
		return;
	}
	if (status==STATUS_OK) {
		status = fs_do_setlength(inode,attrlength,attr);
	}
	if (eptr->version<0x010500) { // should be always false - 'truncate' implemented in mfsmount 1.5
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_TRUNCATE,(status!=STATUS_OK)?5:36);
	} else {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_TRUNCATE,(status!=STATUS_OK)?5:39);
	}
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		if (eptr->version<0x010500) {
			fs_attr_to_attr32(attr,ptr);
		} else {
			memcpy(ptr,attr,35);
		}
	}
	if (eptr->curec) {
		eptr->curec->currentopstats[2]++;
	}
}

void matocuserv_fuse_readlink(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t pleng;
	uint8_t *path;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_READLINK - wrong size (%d/8)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	status = fs_readlink(inode,&pleng,&path);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_READLINK,(status!=STATUS_OK)?5:8+pleng+1);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		PUT32BIT(pleng+1,ptr);
		if (pleng>0) {
			memcpy(ptr,path,pleng);
		}
		ptr[pleng]=0;
	}
	if (eptr->curec) {
		eptr->curec->currentopstats[7]++;
	}
}

void matocuserv_fuse_symlink(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint8_t nleng;
	uint8_t *name,*path;
	uint32_t uid,gid;
	uint32_t pleng;
	uint32_t newinode;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (eptr->registered<3) {
		if (length<276) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_SYMLINK - wrong size (%d/>=276)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode,data);
		name = data;
		data+=256;
		GET32BIT(pleng,data);
		if (length!=276+pleng) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_SYMLINK - wrong size (%d/%d)",length,276+pleng);
			eptr->mode = KILL;
			return;
		}
		path = data;
		data += pleng;
		GET32BIT(uid,data);
		GET32BIT(gid,data);
		nleng = strlen((char*)name);
	} else {
		if (length<21) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_SYMLINK - wrong size (%d)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode,data);
		GET8BIT(nleng,data);
		if (length<21U+nleng) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_SYMLINK - wrong size (%d:nleng=%d)",length,nleng);
			eptr->mode = KILL;
			return;
		}
		name = data;
		data += nleng;
		GET32BIT(pleng,data);
		if (length!=21U+nleng+pleng) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_SYMLINK - wrong size (%d:nleng=%d:pleng=%d)",length,nleng,pleng);
			eptr->mode = KILL;
			return;
		}
		path = data;
		data += pleng;
		GET32BIT(uid,data);
		GET32BIT(gid,data);
	}
	while (pleng>0 && path[pleng-1]==0) {
		pleng--;
	}
	status = fs_symlink(inode,nleng,name,pleng,path,uid,gid,&newinode,attr);
	if (eptr->version<0x010500) {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_SYMLINK,(status!=STATUS_OK)?5:40);
	} else {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_SYMLINK,(status!=STATUS_OK)?5:43);
	}
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		PUT32BIT(newinode,ptr);
		if (eptr->version<0x010500) {
			fs_attr_to_attr32(attr,ptr);
		} else {
			memcpy(ptr,attr,35);
		}
	}
	if (eptr->curec) {
		eptr->curec->currentopstats[6]++;
	}
}

void matocuserv_fuse_mknod(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid,rdev;
	uint8_t nleng;
	uint8_t *name;
	uint8_t type;
	uint16_t mode;
	uint32_t newinode;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (eptr->registered<3) {
		if (length!=279) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_MKNOD - wrong size (%d/279)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode,data);
		name = data;
		data+=256;
		GET8BIT(type,data);
		GET16BIT(mode,data);
		GET32BIT(uid,data);
		GET32BIT(gid,data);
		GET32BIT(rdev,data);
		nleng = strlen((char*)name);
	} else {
		if (length<24) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_MKNOD - wrong size (%d)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode,data);
		GET8BIT(nleng,data);
		if (length!=24U+nleng) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_MKNOD - wrong size (%d:nleng=%d)",length,nleng);
			eptr->mode = KILL;
			return;
		}
		name = data;
		data += nleng;
		GET8BIT(type,data);
		GET16BIT(mode,data);
		GET32BIT(uid,data);
		GET32BIT(gid,data);
		GET32BIT(rdev,data);
	}
	status = fs_mknod(inode,nleng,name,type,mode,uid,gid,rdev,&newinode,attr);
	if (eptr->version<0x010500) {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_MKNOD,(status!=STATUS_OK)?5:40);
	} else {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_MKNOD,(status!=STATUS_OK)?5:43);
	}
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		PUT32BIT(newinode,ptr);
		if (eptr->version<0x010500) {
			fs_attr_to_attr32(attr,ptr);
		} else {
			memcpy(ptr,attr,35);
		}
	}
	if (eptr->curec) {
		eptr->curec->currentopstats[8]++;
	}
}

void matocuserv_fuse_mkdir(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint8_t nleng;
	uint8_t *name;
	uint16_t mode;
	uint32_t newinode;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (eptr->registered<3) {
		if (length!=274) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_MKDIR - wrong size (%d/274)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode,data);
		name = data;
		data+=256;
		GET16BIT(mode,data);
		GET32BIT(uid,data);
		GET32BIT(gid,data);
		nleng = strlen((char*)name);
	} else {
		if (length<19) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_MKDIR - wrong size (%d)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode,data);
		GET8BIT(nleng,data);
		if (length!=19U+nleng) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_MKDIR - wrong size (%d:nleng=%d)",length,nleng);
			eptr->mode = KILL;
			return;
		}
		name = data;
		data += nleng;
		GET16BIT(mode,data);
		GET32BIT(uid,data);
		GET32BIT(gid,data);
	}
	status = fs_mkdir(inode,nleng,name,mode,uid,gid,&newinode,attr);
	if (eptr->version<0x010500) {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_MKDIR,(status!=STATUS_OK)?5:40);
	} else {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_MKDIR,(status!=STATUS_OK)?5:43);
	}
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		PUT32BIT(newinode,ptr);
		if (eptr->version<0x010500) {
			fs_attr_to_attr32(attr,ptr);
		} else {
			memcpy(ptr,attr,35);
		}
	}
	if (eptr->curec) {
		eptr->curec->currentopstats[4]++;
	}
}

void matocuserv_fuse_unlink(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint8_t nleng;
	uint8_t *name;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (eptr->registered<3) {
		if (length!=272) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_UNLINK - wrong size (%d/272)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode,data);
		name = data;
		data+=256;
		GET32BIT(uid,data);
		GET32BIT(gid,data);
		nleng = strlen((char*)name);
	} else {
		if (length<17) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_UNLINK - wrong size (%d)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode,data);
		GET8BIT(nleng,data);
		if (length!=17U+nleng) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_UNLINK - wrong size (%d:nleng=%d)",length,nleng);
			eptr->mode = KILL;
			return;
		}
		name = data;
		data += nleng;
		GET32BIT(uid,data);
		GET32BIT(gid,data);
	}
	status = fs_unlink(inode,nleng,name,uid,gid);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_UNLINK,5);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	PUT8BIT(status,ptr);
	if (eptr->curec) {
		eptr->curec->currentopstats[9]++;
	}
}

void matocuserv_fuse_rmdir(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint8_t nleng;
	uint8_t *name;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (eptr->registered<3) {
		if (length!=272) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_RMDIR - wrong size (%d/272)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode,data);
		name = data;
		data+=256;
		GET32BIT(uid,data);
		GET32BIT(gid,data);
		nleng = strlen((char*)name);
	} else {
		if (length<17) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_RMDIR - wrong size (%d)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode,data);
		GET8BIT(nleng,data);
		if (length!=17U+nleng) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_RMDIR - wrong size (%d:nleng=%d)",length,nleng);
			eptr->mode = KILL;
			return;
		}
		name = data;
		data += nleng;
		GET32BIT(uid,data);
		GET32BIT(gid,data);
	}
	status = fs_rmdir(inode,nleng,name,uid,gid);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_RMDIR,5);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	PUT8BIT(status,ptr);
	if (eptr->curec) {
		eptr->curec->currentopstats[5]++;
	}
}

void matocuserv_fuse_rename(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode_src,inode_dst;
	uint8_t nleng_src,nleng_dst;
	uint8_t *name_src,*name_dst;
	uint32_t uid,gid;
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (eptr->registered<3) {
		if (length!=532) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_RENAME - wrong size (%d/532)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode_src,data);
		name_src = data;
		data+=256;
		GET32BIT(inode_dst,data);
		name_dst = data;
		data+=256;
		GET32BIT(uid,data);
		GET32BIT(gid,data);
		nleng_src = strlen((char*)name_src);
		nleng_dst = strlen((char*)name_dst);
	} else {
		if (length<22) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_RENAME - wrong size (%d)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode_src,data);
		GET8BIT(nleng_src,data);
		if (length<22U+nleng_src) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_RENAME - wrong size (%d:nleng_src=%d)",length,nleng_src);
			eptr->mode = KILL;
			return;
		}
		name_src = data;
		data += nleng_src;
		GET32BIT(inode_dst,data);
		GET8BIT(nleng_dst,data);
		if (length!=22U+nleng_src+nleng_dst) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_RENAME - wrong size (%d:nleng_src=%d:nleng_dst=%d)",length,nleng_src,nleng_dst);
			eptr->mode = KILL;
			return;
		}
		name_dst = data;
		data += nleng_dst;
		GET32BIT(uid,data);
		GET32BIT(gid,data);
	}
	status = fs_rename(inode_src,nleng_src,name_src,inode_dst,nleng_dst,name_dst,uid,gid);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_RENAME,5);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	PUT8BIT(status,ptr);
	if (eptr->curec) {
		eptr->curec->currentopstats[10]++;
	}
}

// na razie funkcja zostanie wy³±czona - do momentu zaimplementowania posiksowego twardego linka
void matocuserv_fuse_link(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode,inode_dst;
	uint8_t nleng_dst;
	uint8_t *name_dst;
	uint32_t uid,gid;
	uint32_t newinode;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (eptr->registered<3) {
		if (length!=276) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_LINK - wrong size (%d/276)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode,data);
		GET32BIT(inode_dst,data);
		name_dst = data;
		data+=256;
		GET32BIT(uid,data);
		GET32BIT(gid,data);
		nleng_dst = strlen((char*)name_dst);
	} else {
		if (length<21) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_LINK - wrong size (%d)",length);
			eptr->mode = KILL;
			return;
		}
		GET32BIT(msgid,data);
		GET32BIT(inode,data);
		GET32BIT(inode_dst,data);
		GET8BIT(nleng_dst,data);
		if (length!=21U+nleng_dst) {
			syslog(LOG_NOTICE,"CUTOMA_FUSE_LINK - wrong size (%d:nleng_dst=%d)",length,nleng_dst);
			eptr->mode = KILL;
			return;
		}
		name_dst = data;
		data += nleng_dst;
		GET32BIT(uid,data);
		GET32BIT(gid,data);
	}
	status = fs_link(inode,inode_dst,nleng_dst,name_dst,uid,gid,&newinode,attr);
	if (eptr->version<0x010500) {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_LINK,(status!=STATUS_OK)?5:40);
	} else {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_LINK,(status!=STATUS_OK)?5:43);
	}
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		PUT32BIT(newinode,ptr);
		if (eptr->version<0x010500) {
			fs_attr_to_attr32(attr,ptr);
		} else {
			memcpy(ptr,attr,35);
		}
	}
	if (eptr->curec) {
		eptr->curec->currentopstats[11]++;
	}
}

void matocuserv_fuse_getdir(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint32_t dleng;
	void *custom;
	if (length!=16) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_GETDIR - wrong size (%d/16)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	GET32BIT(uid,data);
	GET32BIT(gid,data);
	status = fs_readdir_size(inode,uid,gid,&custom,&dleng,(eptr->registered<3)?0:1);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_GETDIR,(status!=STATUS_OK)?5:4+dleng);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		fs_readdir_data(custom,ptr,(eptr->registered<3)?0:1);
	}
	if (eptr->curec) {
		eptr->curec->currentopstats[12]++;
	}
}

void matocuserv_fuse_open(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid;
	uint8_t flags;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=17) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_OPEN - wrong size (%d/17)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	GET32BIT(uid,data);
	GET32BIT(gid,data);
	GET8BIT(flags,data);
	if (eptr->registered>=3) {
		status = matocuserv_insert_openfile(eptr->curec,inode);
	} else {
		status = STATUS_OK;
	}
	if (status==STATUS_OK) {
		status = fs_opencheck(inode,uid,gid,flags);
	}
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_OPEN,5);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	PUT8BIT(status,ptr);
	if (eptr->curec) {
		eptr->curec->currentopstats[13]++;
	}
}

void matocuserv_fuse_read_chunk(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t status;
	uint32_t inode;
	uint32_t indx;
	uint64_t chunkid;
	uint64_t fleng;
	uint32_t version;
	uint32_t ip;
	uint16_t port;
	uint8_t i,count;
	void *sptr[256];
	uint32_t msgid;
	if (length!=12) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_READ_CHUNK - wrong size (%d/12)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	GET32BIT(indx,data);
//	if (matocuserv_open_check(eptr,inode)<0) {
//		status = ERROR_NOTOPENED;
//	} else {
		status = fs_readchunk(inode,indx,&chunkid,&fleng);
//	}
	if (status==STATUS_OK) {
		if (chunkid>0) {
			status = chunk_getversionandlocations(chunkid,&version,&count,sptr);
		} else {
			version = 0;
			count = 0;
		}
	}
	if (status!=STATUS_OK) {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_READ_CHUNK,5);
		if (ptr==NULL) {
			syslog(LOG_NOTICE,"can't allocate memory for packet");
			eptr->mode = KILL;
			return;
		}
		PUT32BIT(msgid,ptr);
		PUT8BIT(status,ptr);
		return;
	}
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_READ_CHUNK,24+count*6);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	PUT64BIT(fleng,ptr);
	PUT64BIT(chunkid,ptr);
	PUT32BIT(version,ptr);
	for (i=0 ; i<count ; i++) {
		if (matocsserv_getlocation(sptr[i],&ip,&port)<0) {
			PUT32BIT(0,ptr);
			PUT16BIT(0,ptr);
		} else {
			PUT32BIT(ip,ptr);
			PUT16BIT(port,ptr);
		}
	}
	if (eptr->curec) {
		eptr->curec->currentopstats[14]++;
	}
}

void matocuserv_fuse_write_chunk(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t status;
	uint32_t inode;
	uint32_t indx;
	uint64_t fleng;
	uint64_t chunkid;
	uint32_t msgid;
	chunklist *cl;
	if (length!=12) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_WRITE_CHUNK - wrong size (%d/12)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	GET32BIT(indx,data);
	status = fs_writechunk(inode,indx,&chunkid,&fleng);
/* zapis bez zwiekszania wersji
	if (status==255) {
		uint32_t version;
		uint32_t ip;
		uint16_t port;
		uint8_t i,count;
		void *sptr[256];
		status=chunk_getversionandlocations(chunkid,&version,&count,sptr);
		if (status!=STATUS_OK) {
			ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_WRITE_CHUNK,5);
			if (ptr==NULL) {
				syslog(LOG_NOTICE,"can't allocate memory for packet");
				eptr->mode = KILL;
				return;
			}
			PUT32BIT(msgid,ptr);
			PUT8BIT(status,ptr);
			fs_writeend(0,0,chunkid);	// ignore status - just do it.
			return;
		}
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_WRITE_CHUNK,24+count*6);
		if (ptr==NULL) {
			syslog(LOG_NOTICE,"can't allocate memory for packet");
			eptr->mode = KILL;
			return;
		}
		PUT32BIT(msgid,ptr);
		PUT64BIT(fleng,ptr);
		PUT64BIT(chunkid,ptr);
		PUT32BIT(version,ptr);
		for (i=0 ; i<count ; i++) {
			if (matocsserv_getlocation(sptr[i],&ip,&port)<0) {
				PUT32BIT(0,ptr);
				PUT16BIT(0,ptr);
			} else {
				PUT32BIT(ip,ptr);
				PUT16BIT(port,ptr);
			}
		}
		return;
	}
*/
	if (status!=STATUS_OK) {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_WRITE_CHUNK,5);
		if (ptr==NULL) {
			syslog(LOG_NOTICE,"can't allocate memory for packet");
			eptr->mode = KILL;
			return;
		}
		PUT32BIT(msgid,ptr);
		PUT8BIT(status,ptr);
		return;
	}
	cl = (chunklist*)malloc(sizeof(chunklist));
	cl->chunkid = chunkid;
	cl->qid = msgid;
	cl->fleng = fleng;
	cl->type = FUSE_WRITE;
	cl->next = eptr->chunkdelayedops;
	eptr->chunkdelayedops = cl;
	if (eptr->curec) {
		eptr->curec->currentopstats[15]++;
	}
}

void matocuserv_fuse_write_chunk_end(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint32_t msgid;
	uint32_t inode;
	uint64_t fleng;
	uint64_t chunkid;
	uint8_t status;
//	chunklist *cl,**acl;
	if (length!=24) {
		syslog(LOG_NOTICE,"CUTOMA_WRITE_CHUNK_END - wrong size (%d/24)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET64BIT(chunkid,data);
	GET32BIT(inode,data);
	GET64BIT(fleng,data);
	status = fs_writeend(inode,fleng,chunkid);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_WRITE_CHUNK_END,5);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	PUT8BIT(status,ptr);
}

void matocuserv_fuse_check(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint16_t t16,chunkcount[256];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_CHECK - wrong size (%d/8)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	status = fs_checkfile(inode,chunkcount);
	if (status!=STATUS_OK) {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_CHECK,5);
		if (ptr==NULL) {
			syslog(LOG_NOTICE,"can't allocate memory for packet");
			eptr->mode = KILL;
			return;
		}
		PUT32BIT(msgid,ptr);
		PUT8BIT(status,ptr);
	} else {
		uint32_t i,j;
		j=0;
		for (i=0 ; i<256 ; i++) {
			if (chunkcount[i]>0) {
				j++;
			}
		}
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_CHECK,4+j*3);
		if (ptr==NULL) {
			syslog(LOG_NOTICE,"can't allocate memory for packet");
			eptr->mode = KILL;
			return;
		}
		PUT32BIT(msgid,ptr);
		for (i=0 ; i<256 ; i++) {
			t16 = chunkcount[i];
			if (t16>0) {
				PUT8BIT(i,ptr);
				PUT16BIT(t16,ptr);
			}
		}
	}
}


void matocuserv_fuse_gettrashtime(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint8_t gmode;
	void *fptr,*dptr;
	uint32_t fnodes,dnodes;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=9) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_GETTRASHTIME - wrong size (%d/9)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	GET8BIT(gmode,data);
	status = fs_gettrashtime_prepare(inode,gmode,&fptr,&dptr,&fnodes,&dnodes);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_GETTRASHTIME,(status!=STATUS_OK)?5:12+8*(fnodes+dnodes));
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		PUT32BIT(fnodes,ptr);
		PUT32BIT(dnodes,ptr);
		fs_gettrashtime_store(fptr,dptr,ptr);
	}
}

void matocuserv_fuse_settrashtime(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode,uid,trashtime;
	uint32_t msgid;
	uint8_t smode;
	uint32_t changed,notchanged,notpermitted;
	uint8_t *ptr;
	uint8_t status;
	if (length!=17) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_SETTRASHTIME - wrong size (%d/17)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	GET32BIT(uid,data);
	GET32BIT(trashtime,data);
	GET8BIT(smode,data);
	status = fs_settrashtime(inode,uid,trashtime,smode,&changed,&notchanged,&notpermitted);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_SETTRASHTIME,(status!=STATUS_OK)?5:16);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		PUT32BIT(changed,ptr);
		PUT32BIT(notchanged,ptr);
		PUT32BIT(notpermitted,ptr);
	}
}

void matocuserv_fuse_getgoal(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint32_t fgtab[10],dgtab[10];
	uint8_t i,fn,dn,gmode;
	uint8_t *ptr;
	uint8_t status;
	if (length!=9) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_GETGOAL - wrong size (%d/9)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	GET8BIT(gmode,data);
	status = fs_getgoal(inode,gmode,fgtab,dgtab);
	fn=0;
	dn=0;
	if (status==STATUS_OK) {
		for (i=1 ; i<10 ; i++) {
			if (fgtab[i]) {
				fn++;
			}
			if (dgtab[i]) {
				dn++;
			}
		}
	}
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_GETGOAL,(status!=STATUS_OK)?5:6+5*(fn+dn));
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		PUT8BIT(fn,ptr);
		PUT8BIT(dn,ptr);
		for (i=1 ; i<10 ; i++) {
			if (fgtab[i]) {
				PUT8BIT(i,ptr);
				PUT32BIT(fgtab[i],ptr);
			}
		}
		for (i=1 ; i<10 ; i++) {
			if (dgtab[i]) {
				PUT8BIT(i,ptr);
				PUT32BIT(dgtab[i],ptr);
			}
		}
	}
}

void matocuserv_fuse_setgoal(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode,uid;
	uint32_t msgid;
	uint8_t goal,smode;
	uint32_t changed,notchanged,notpermitted;
	uint8_t *ptr;
	uint8_t status;
	if (length!=14) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_SETGOAL - wrong size (%d/14)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	GET32BIT(uid,data);
	GET8BIT(goal,data);
	GET8BIT(smode,data);
	status = fs_setgoal(inode,uid,goal,smode,&changed,&notchanged,&notpermitted);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_SETGOAL,(status!=STATUS_OK)?5:16);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		PUT32BIT(changed,ptr);
		PUT32BIT(notchanged,ptr);
		PUT32BIT(notpermitted,ptr);
	}
}

void matocuserv_fuse_append(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode,inode_src,uid,gid;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=20) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_APPEND - wrong size (%d/20)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	GET32BIT(inode_src,data);
	GET32BIT(uid,data);
	GET32BIT(gid,data);
	status = fs_append(inode,inode_src,uid,gid);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_APPEND,5);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	PUT8BIT(status,ptr);
}

void matocuserv_fuse_getdirstats(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode,inodes,files,dirs,ugfiles,mfiles,chunks,ugchunks,mchunks;
	uint64_t leng,size,gsize;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_GETDIRSTATS - wrong size (%d/8)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	status = fs_get_dir_stats(inode,&inodes,&dirs,&files,&ugfiles,&mfiles,&chunks,&ugchunks,&mchunks,&leng,&size,&gsize);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_GETDIRSTATS,(status!=STATUS_OK)?5:60);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		PUT32BIT(inodes,ptr);
		PUT32BIT(dirs,ptr);
		PUT32BIT(files,ptr);
		PUT32BIT(ugfiles,ptr);
		PUT32BIT(mfiles,ptr);
		PUT32BIT(chunks,ptr);
		PUT32BIT(ugchunks,ptr);
		PUT32BIT(mchunks,ptr);
		PUT64BIT(leng,ptr);
		PUT64BIT(size,ptr);
		PUT64BIT(gsize,ptr);
	}
}

void matocuserv_fuse_gettrash(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t *ptr;
	uint32_t dleng;
	if (length!=4) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_GETTRASH - wrong size (%d/4)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	dleng = fs_readtrash_size((eptr->registered<3)?0:1);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_GETTRASH,4+dleng);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	fs_readtrash_data(ptr,(eptr->registered<3)?0:1);
}

void matocuserv_fuse_getdetachedattr(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint8_t attr[35];
	uint32_t msgid;
	uint8_t dtype;
	uint8_t *ptr;
	uint8_t status;
	if (length<8 || length>9) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_GETDETACHEDATTR - wrong size (%d/8,9)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	if (length==9) {
		GET8BIT(dtype,data);
	} else {
		dtype = DTYPE_UNKNOWN;
	}
	status = fs_getdetachedattr(inode,attr,dtype);
	if (eptr->version<0x010500) {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_GETDETACHEDATTR,(status!=STATUS_OK)?5:36);
	} else {
		ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_GETDETACHEDATTR,(status!=STATUS_OK)?5:39);
	}
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		if (eptr->version<0x010500) {
			fs_attr_to_attr32(attr,ptr);
		} else {
			memcpy(ptr,attr,35);
		}
	}
}

void matocuserv_fuse_gettrashpath(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t pleng;
	uint8_t *path;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_GETTRASHPATH - wrong size (%d/8)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	status = fs_gettrashpath(inode,&pleng,&path);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_GETTRASHPATH,(status!=STATUS_OK)?5:8+pleng+1);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		PUT32BIT(pleng+1,ptr);
		if (pleng>0) {
			memcpy(ptr,path,pleng);
		}
		ptr[pleng]=0;
	}
}

void matocuserv_fuse_settrashpath(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint8_t *path;
	uint32_t pleng;
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (length<12) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_SETTRASHPATH - wrong size (%d/>=12)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	GET32BIT(pleng,data);
	if (length!=12+pleng) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_SETTRASHPATH - wrong size (%d/%d)",length,12+pleng);
		eptr->mode = KILL;
		return;
	}
	path = data;
	data += pleng;
	while (pleng>0 && path[pleng-1]==0) {
		pleng--;
	}
	status = fs_settrashpath(inode,pleng,path);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_SETTRASHPATH,5);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	PUT8BIT(status,ptr);
}

void matocuserv_fuse_undel(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (length!=8) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_UNDEL - wrong size (%d/8)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	status = fs_undel(inode);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_UNDEL,5);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	PUT8BIT(status,ptr);
}

void matocuserv_fuse_purge(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_PURGE - wrong size (%d/8)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	GET32BIT(inode,data);
	status = fs_purge(inode);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_PURGE,5);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	PUT8BIT(status,ptr);
}


void matocuserv_fuse_getreserved(matocuserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t *ptr;
	uint32_t dleng;
	if (length!=4) {
		syslog(LOG_NOTICE,"CUTOMA_FUSE_GETRESERVED - wrong size (%d/4)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(msgid,data);
	dleng = fs_readreserved_size((eptr->registered<3)?0:1);
	ptr = matocuserv_createpacket(eptr,MATOCU_FUSE_GETRESERVED,4+dleng);
	if (ptr==NULL) {
		syslog(LOG_NOTICE,"can't allocate memory for packet");
		eptr->mode = KILL;
		return;
	}
	PUT32BIT(msgid,ptr);
	fs_readreserved_data(ptr,(eptr->registered<3)?0:1);
}



void matocu_customer_timedout(custrecord *curec) {
	filelist *fl,*afl;
	fl=curec->openedfiles; 
	while (fl) {
		afl = fl;
		fl=fl->next;
		fs_release(afl->inode,curec->cuid);
		free(afl);
	}
	curec->openedfiles=NULL;
}

void matocu_customer_check(void) {
	custrecord **curec,*acurec;
	uint32_t now;

	now = main_time();
	curec = &(curechead);
	while ((acurec=*curec)) {
		if (acurec->nsocks==0 && acurec->disconnected+CURECORD_TIMEOUT<now) {
			matocu_customer_timedout(acurec);
			*curec = acurec->next;
			free(acurec);
		} else {
			curec = &(acurec->next);
		}
	}
}

void matocu_customer_statsmove(void) {
	custrecord *curec;
	for (curec = curechead ; curec ; curec=curec->next) {
		memcpy(curec->lasthouropstats,curec->currentopstats,4*16);
		memset(curec->currentopstats,0,4*16);
	}
}

void matocu_beforedisconnect(matocuserventry *eptr) {
	chunklist *cl,*acl;
// unlock locked chunks
	cl=eptr->chunkdelayedops; 
	while (cl) {
		acl = cl;
		cl=cl->next;
		if (acl->type == FUSE_SETATTR || acl->type == FUSE_TRUNCATE) {
			fs_end_setlength(acl->chunkid);
		}
		free(acl);
	}
	eptr->chunkdelayedops=NULL;
	if (eptr->curec) {
		if (eptr->curec->nsocks>0) {
			eptr->curec->nsocks--;
		}
		if (eptr->curec->nsocks==0) {
			eptr->curec->disconnected = main_time();
		}
	}
}

void matocuserv_gotpacket(matocuserventry *eptr,uint32_t type,uint8_t *data,uint32_t length) {
	if (eptr->registered==0) {
		switch (type) {
			case ANTOAN_NOP:
				break;
			case CUTOMA_FUSE_REGISTER:
				matocuserv_fuse_register(eptr,data,length);
				break;
			case CUTOMA_CSERV_LIST:
				matocuserv_cserv_list(eptr,data,length);
				break;
			case CUTOMA_CUST_LIST:
				matocuserv_cust_list(eptr,data,length);
				break;
			case CUTOAN_CHART:
				matocuserv_chart(eptr,data,length);
				break;
			case CUTOAN_CHART_DATA:
				matocuserv_chart_data(eptr,data,length);
				break;
			case CUTOMA_INFO:
				matocuserv_info(eptr,data,length);
				break;
			case CUTOMA_FSTEST_INFO:
				matocuserv_fstest_info(eptr,data,length);
				break;
			case CUTOMA_CHUNKSTEST_INFO:
				matocuserv_chunkstest_info(eptr,data,length);
				break;
			default:
				syslog(LOG_NOTICE,"matocu: got unknown message from unregistered (type:%d)",type);
				eptr->mode=KILL;
		}
	} else {
		switch (type) {
			case ANTOAN_NOP:
				break;
			case CUTOMA_FUSE_RESERVED_INODES:
				matocuserv_fuse_reserved_inodes(eptr,data,length);
				break;
			case CUTOMA_FUSE_STATFS:
				matocuserv_fuse_statfs(eptr,data,length);
				break;
			case CUTOMA_FUSE_ACCESS:
				matocuserv_fuse_access(eptr,data,length);
				break;
			case CUTOMA_FUSE_LOOKUP:
				matocuserv_fuse_lookup(eptr,data,length);
				break;
			case CUTOMA_FUSE_GETATTR:
				matocuserv_fuse_getattr(eptr,data,length);
				break;
			case CUTOMA_FUSE_SETATTR:
				matocuserv_fuse_setattr(eptr,data,length);
				break;
			case CUTOMA_FUSE_READLINK:
				matocuserv_fuse_readlink(eptr,data,length);
				break;
			case CUTOMA_FUSE_SYMLINK:
				matocuserv_fuse_symlink(eptr,data,length);
				break;
			case CUTOMA_FUSE_MKNOD:
				matocuserv_fuse_mknod(eptr,data,length);
				break;
			case CUTOMA_FUSE_MKDIR:
				matocuserv_fuse_mkdir(eptr,data,length);
				break;
			case CUTOMA_FUSE_UNLINK:
				matocuserv_fuse_unlink(eptr,data,length);
				break;
			case CUTOMA_FUSE_RMDIR:
				matocuserv_fuse_rmdir(eptr,data,length);
				break;
			case CUTOMA_FUSE_RENAME:
				matocuserv_fuse_rename(eptr,data,length);
				break;
			case CUTOMA_FUSE_LINK:
				matocuserv_fuse_link(eptr,data,length);
				break;
			case CUTOMA_FUSE_GETDIR:
				matocuserv_fuse_getdir(eptr,data,length);
				break;

			case CUTOMA_FUSE_OPEN:
				matocuserv_fuse_open(eptr,data,length);
				break;
			case CUTOMA_FUSE_READ_CHUNK:
				matocuserv_fuse_read_chunk(eptr,data,length);
				break;
			case CUTOMA_FUSE_WRITE_CHUNK:
				matocuserv_fuse_write_chunk(eptr,data,length);
				break;
			case CUTOMA_FUSE_WRITE_CHUNK_END:
				matocuserv_fuse_write_chunk_end(eptr,data,length);
				break;
// extra (external tools)
			case CUTOMA_FUSE_CHECK:
				matocuserv_fuse_check(eptr,data,length);
				break;
			case CUTOMA_FUSE_GETTRASHTIME:
				matocuserv_fuse_gettrashtime(eptr,data,length);
				break;
			case CUTOMA_FUSE_SETTRASHTIME:
				matocuserv_fuse_settrashtime(eptr,data,length);
				break;
			case CUTOMA_FUSE_GETGOAL:
				matocuserv_fuse_getgoal(eptr,data,length);
				break;
			case CUTOMA_FUSE_SETGOAL:
				matocuserv_fuse_setgoal(eptr,data,length);
				break;
			case CUTOMA_FUSE_APPEND:
				matocuserv_fuse_append(eptr,data,length);
				break;
			case CUTOMA_FUSE_GETDIRSTATS:
				matocuserv_fuse_getdirstats(eptr,data,length);
				break;
			case CUTOMA_FUSE_TRUNCATE:
				matocuserv_fuse_truncate(eptr,data,length);
				break;
// fuse - meta
			case CUTOMA_FUSE_GETTRASH:
				matocuserv_fuse_gettrash(eptr,data,length);
				break;
			case CUTOMA_FUSE_GETDETACHEDATTR:
				matocuserv_fuse_getdetachedattr(eptr,data,length);
				break;
			case CUTOMA_FUSE_GETTRASHPATH:
				matocuserv_fuse_gettrashpath(eptr,data,length);
				break;
			case CUTOMA_FUSE_SETTRASHPATH:
				matocuserv_fuse_settrashpath(eptr,data,length);
				break;
			case CUTOMA_FUSE_UNDEL:
				matocuserv_fuse_undel(eptr,data,length);
				break;
			case CUTOMA_FUSE_PURGE:
				matocuserv_fuse_purge(eptr,data,length);
				break;
			case CUTOMA_FUSE_GETRESERVED:
				matocuserv_fuse_getreserved(eptr,data,length);
				break;
/* - zwroci liste klientow (ip:port), ktorzy zarezerwowali dany obiekt
			case CUTOMA_FUSE_GETRESERVED_CULIST:
				matocuserv_fuse_getreserved_culist(eptr,data,length);
				break;
*/
			default:
				syslog(LOG_NOTICE,"matocu: got unknown message (type:%d)",type);
				eptr->mode=KILL;
		}
	}
}

void matocuserv_term(void) {
	matocuserventry *eptr,*eaptr;
	packetstruct *pptr,*paptr;
	syslog(LOG_INFO,"matocu: closing %s:%s",ListenHost,ListenPort);
	tcpclose(lsock);

	eptr = matocuservhead;
	while (eptr) {
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
		eaptr = eptr;
		eptr = eptr->next;
		free(eaptr);
	}
	matocuservhead=NULL;
}

void matocuserv_read(matocuserventry *eptr) {
	int32_t i;
	uint32_t type,size;
	uint8_t *ptr;
	i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
	if (i==0) {
		// syslog(LOG_INFO,"matocu: connection lost");
		eptr->mode = KILL;
		return;
	}
	if (i<0) {
		syslog(LOG_INFO,"matocu: read error: %m");
		eptr->mode = KILL;
		return;
	}
	eptr->inputpacket.startptr+=i;
	eptr->inputpacket.bytesleft-=i;

	if (eptr->inputpacket.bytesleft>0) {
		return;
	}

	if (eptr->mode==HEADER) {
		ptr = eptr->hdrbuff+4;
		GET32BIT(size,ptr);

		if (size>0) {
			if (size>MaxPacketSize) {
				syslog(LOG_WARNING,"matocu: packet too long (%u/%u)",size,MaxPacketSize);
				eptr->mode = KILL;
				return;
			}
			eptr->inputpacket.packet = malloc(size);
			if (eptr->inputpacket.packet==NULL) {
				syslog(LOG_WARNING,"matocu: out of memory");
				eptr->mode = KILL;
				return;
			}
			eptr->inputpacket.bytesleft = size;
			eptr->inputpacket.startptr = eptr->inputpacket.packet;
			eptr->mode = DATA;
			return;
		}
		eptr->mode = DATA;
	}

	if (eptr->mode==DATA) {
		ptr = eptr->hdrbuff;
		GET32BIT(type,ptr);
		GET32BIT(size,ptr);

		eptr->mode=HEADER;
		eptr->inputpacket.bytesleft = 8;
		eptr->inputpacket.startptr = eptr->hdrbuff;

		matocuserv_gotpacket(eptr,type,eptr->inputpacket.packet,size);

		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		eptr->inputpacket.packet=NULL;
		return;
	}
}

void matocuserv_write(matocuserventry *eptr) {
	packetstruct *pack;
	int32_t i;
	pack = eptr->outputhead;
	if (pack==NULL) {
		return;
	}
	i=write(eptr->sock,pack->startptr,pack->bytesleft);
	if (i<0) {
		syslog(LOG_INFO,"matocu: write error: %m");
		eptr->mode = KILL;
		return;
	}
	pack->startptr+=i;
	pack->bytesleft-=i;
	if (pack->bytesleft>0) {
		return;
	}
	free(pack->packet);
	eptr->outputhead = pack->next;
	if (eptr->outputhead==NULL) {
		eptr->outputtail = &(eptr->outputhead);
	}
	free(pack);
}

void matocuserv_wantexit(void) {
	exiting=1;
}

int matocuserv_canexit(void) {
	matocuserventry *eptr;
	for (eptr=matocuservhead ; eptr ; eptr=eptr->next) {
		if (eptr->outputhead!=NULL) {
			return 0;
		}
		if (eptr->chunkdelayedops!=NULL) {
			return 0;
		}
	}
	return 1;
}

int matocuserv_desc(fd_set *rset,fd_set *wset) {
	int max;
	matocuserventry *eptr;
	int i;

	if (exiting==0) {
		FD_SET(lsock,rset);
		max = lsock;
	} else {
		max = -1;
	}
	for (eptr=matocuservhead ; eptr ; eptr=eptr->next) {
		i=eptr->sock;
		if (exiting==0) {
			FD_SET(i,rset);
			if (i>max) {
				max=i;
			}
		}
		if (eptr->outputhead!=NULL) {
			FD_SET(i,wset);
			if (i>max) {
				max=i;
			}
		}
	}
	return max;
}


void matocuserv_serve(fd_set *rset,fd_set *wset) {
	uint32_t now=main_time();
	matocuserventry *eptr,**kptr;
	packetstruct *pptr,*paptr;
	int ns;

	if (FD_ISSET(lsock,rset)) {
		ns=tcpaccept(lsock);
		if (ns<0) {
			syslog(LOG_INFO,"matocu: accept error: %m");
		} else {
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = malloc(sizeof(matocuserventry));
			eptr->next = matocuservhead;
			matocuservhead = eptr;
			eptr->sock = ns;
			eptr->registered = 0;
			eptr->version = 0;
			eptr->mode = HEADER;
			eptr->lastread = now;
			eptr->lastwrite = now;
			eptr->inputpacket.next = NULL;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;
			eptr->inputpacket.packet = NULL;
			eptr->outputhead = NULL;
			eptr->outputtail = &(eptr->outputhead);

			eptr->chunkdelayedops = NULL;
			eptr->curec = NULL;
//			eptr->openedfiles = NULL;
		}
	}

	for (eptr=matocuservhead ; eptr ; eptr=eptr->next) {
		if (FD_ISSET(eptr->sock,rset) && eptr->mode!=KILL) {
			eptr->lastread = now;
			matocuserv_read(eptr);
		}
		if (FD_ISSET(eptr->sock,wset) && eptr->mode!=KILL) {
			eptr->lastwrite = now;
			matocuserv_write(eptr);
		}
		if (eptr->registered==1) {
			if (eptr->lastread+60<now && exiting==0) {	// for old fuse
				eptr->mode = KILL;
			}
		} else {
			if (eptr->lastread+10<now && exiting==0) {
				eptr->mode = KILL;
			}
			if (eptr->lastwrite+2<now && eptr->registered>=2 && eptr->outputhead==NULL) {
				uint8_t *ptr = matocuserv_createpacket(eptr,ANTOAN_NOP,4);	// 4 byte length because of 'msgid'
				*((uint32_t*)ptr) = 0;
			}
		}
	}
	kptr = &matocuservhead;
	while ((eptr=*kptr)) {
		if (eptr->mode == KILL) {
			matocu_beforedisconnect(eptr);
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
			free(eptr);
		} else {
			kptr = &(eptr->next);
		}
	}
}

int matocuserv_init(void) {
	config_getnewstr("MATOCU_LISTEN_HOST","*",&ListenHost);
	config_getnewstr("MATOCU_LISTEN_PORT","9421",&ListenPort);
//	config_getuint32("MATOCU_TIMEOUT",4,&Timeout);

	exiting = 0;
	lsock = tcpsocket();
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	tcplisten(lsock,ListenHost,ListenPort,5);
	if (lsock<0) {
		syslog(LOG_ERR,"matocu: listen error: %m");
		return -1;
	}
	syslog(LOG_NOTICE,"matocu: listen on %s:%s",ListenHost,ListenPort);

	curechead = NULL;
	matocuservhead = NULL;

	main_timeregister(TIMEMODE_RUNONCE,10,0,matocu_customer_check);
	main_timeregister(TIMEMODE_RUNONCE,3600,0,matocu_customer_statsmove);

	main_destructregister(matocuserv_term);
	main_selectregister(matocuserv_desc,matocuserv_serve);
	main_wantexitregister(matocuserv_wantexit);
	main_canexitregister(matocuserv_canexit);
	return 0;
}
