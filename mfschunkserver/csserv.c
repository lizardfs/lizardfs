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
#include "csserv.h"
#include "cfg.h"
#include "main.h"
#include "sockets.h"
#include "hddspacemgr.h"
#include "cstocsconn.h"
#include "stats.h"

#define MaxPacketSize 100000

//csserventry.mode
enum {KILL,HEADER,DATA};
//csserventry.operation
enum {IDLE,READING,CONNECTING,WRITING,WRITE_ERROR};

typedef struct packetstruct {
	struct packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t *packet;
} packetstruct;

typedef struct csserventry {
	uint8_t mode;
	int sock;
	time_t activity;
	uint8_t hdrbuff[8];
	packetstruct inputpacket;
	packetstruct *outputhead,**outputtail;

	uint8_t operation;		// idle / reading / connecting / writting / waiting

	uint64_t chunkid;		// R+W
	uint32_t version;		// R+W
	uint32_t offset;		// R
	uint32_t size;			// R
	uint8_t *chain;			// W
	uint32_t chainleng;		// W
	void *conn;			// W
//	uint32_t blocknum;		// for read operation

	struct csserventry *next;
} csserventry;

static csserventry *csservhead=NULL;
static int lsock;

static uint32_t mylistenip;
static uint16_t mylistenport;

static uint32_t stats_bytesin=0;
static uint32_t stats_bytesout=0;
static uint32_t stats_hlopr=0;
static uint32_t stats_hlopw=0;

// from config
static char *ListenHost;
static char *ListenPort;
static uint32_t Timeout;

void csserv_stats(uint32_t *bin,uint32_t *bout,uint32_t *hlopr,uint32_t *hlopw) {
	*bin = stats_bytesin;
	*bout = stats_bytesout;
	*hlopr = stats_hlopr;
	*hlopw = stats_hlopw;
	stats_bytesin = 0;
	stats_bytesout = 0;
	stats_hlopr = 0;
	stats_hlopw = 0;
}

int csserv_deletepacket(csserventry *eptr) {
	packetstruct *pptr;
	if (eptr->outputhead==NULL) {
		return -1;
	}
	eptr->outputtail = &(eptr->outputhead);
	while ((pptr = *(eptr->outputtail))) {
		if (pptr->next==NULL) {
			free(pptr->packet);
			free(pptr);
			*(eptr->outputtail) = NULL;
		} else {
			eptr->outputtail = &(pptr->next);
		}
	}
	return 0;
}

uint8_t* csserv_createpacket(csserventry *eptr,uint32_t type,uint32_t size) {
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

// reading

void csserv_read_continue(csserventry *eptr) {
	uint16_t blocknum;
	uint16_t blockoffset;
	uint32_t size;
	uint32_t crc;
	uint8_t *ptr;
	uint8_t status;

	blocknum = (eptr->offset)>>16;
	blockoffset = (eptr->offset)&0xFFFF;
	if ((eptr->offset+eptr->size-1)>>16 == blocknum) {	// last block
		size = eptr->size;
		ptr = csserv_createpacket(eptr,CSTOCU_READ_DATA,8+2+2+4+4+size);
		if (ptr==NULL) {
			eptr->mode = KILL;
			return;
		}
		PUT64BIT(eptr->chunkid,ptr);
		PUT16BIT(blocknum,ptr);
		PUT16BIT(blockoffset,ptr);
		PUT32BIT(size,ptr);
		status = read_block_from_chunk(eptr->chunkid,eptr->version,blocknum,ptr+4,blockoffset,size,&crc);
		PUT32BIT(crc,ptr);
		if (status!=STATUS_OK) {
			csserv_deletepacket(eptr);
		}
		ptr = csserv_createpacket(eptr,CSTOCU_READ_STATUS,8+1);
		if (ptr==NULL) {
			eptr->mode = KILL;
			return;
		}
		PUT64BIT(eptr->chunkid,ptr);
		PUT8BIT(status,ptr);
		chunk_after_io(eptr->chunkid);
		eptr->operation = IDLE;
	} else {
		size = 0x10000-blockoffset;
		ptr = csserv_createpacket(eptr,CSTOCU_READ_DATA,8+2+2+4+4+size);
		if (ptr==NULL) {
			eptr->mode = KILL;
			return;
		}
		PUT64BIT(eptr->chunkid,ptr);
		PUT16BIT(blocknum,ptr);
		PUT16BIT(blockoffset,ptr);
		PUT32BIT(size,ptr);
		status = read_block_from_chunk(eptr->chunkid,eptr->version,blocknum,ptr+4,blockoffset,size,&crc);
		PUT32BIT(crc,ptr);
		if (status!=STATUS_OK) {
			csserv_deletepacket(eptr);
			ptr = csserv_createpacket(eptr,CSTOCU_READ_STATUS,8+1);
			if (ptr==NULL) {
				eptr->mode = KILL;
				return;
			}
			PUT64BIT(eptr->chunkid,ptr);
			PUT8BIT(status,ptr);
			chunk_after_io(eptr->chunkid);
			eptr->operation = IDLE;
		} else {
			eptr->offset+=size;
			eptr->size-=size;
		}
	}
}

void csserv_read_init(csserventry *eptr,uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t status;

	if (eptr->operation!=IDLE) {
		eptr->mode = KILL;
		return;
	}
	if (length!=8+4+4+4) {
		syslog(LOG_NOTICE,"CUTOCS_READ - wrong size (%"PRIu32"/20)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(eptr->chunkid,data);
	GET32BIT(eptr->version,data);
	GET32BIT(eptr->offset,data);
	GET32BIT(eptr->size,data);
	status = check_chunk(eptr->chunkid,eptr->version);
	if (status!=STATUS_OK) {
		ptr = csserv_createpacket(eptr,CSTOCU_READ_STATUS,8+1);
		if (ptr==NULL) {
			eptr->mode = KILL;
			return ;
		}
		PUT64BIT(eptr->chunkid,ptr);
		PUT8BIT(status,ptr);
		return;
	}
	if (eptr->size>0x4000000 || eptr->size==0) {
		ptr = csserv_createpacket(eptr,CSTOCU_READ_STATUS,8+1);
		if (ptr==NULL) {
			eptr->mode = KILL;
			return ;
		}
		PUT64BIT(eptr->chunkid,ptr);
		PUT8BIT(ERROR_WRONGSIZE,ptr);
		return;
	}
	if (eptr->offset>=0x4000000 || eptr->offset+eptr->size>0x4000000) {
		ptr = csserv_createpacket(eptr,CSTOCU_READ_STATUS,8+1);
		if (ptr==NULL) {
			eptr->mode = KILL;
			return ;
		}
		PUT64BIT(eptr->chunkid,ptr);
		PUT8BIT(ERROR_WRONGOFFSET,ptr);
		return;
	}
	status = chunk_before_io(eptr->chunkid);
	if (status!=STATUS_OK) {
		ptr = csserv_createpacket(eptr,CSTOCU_READ_STATUS,8+1);
		if (ptr==NULL) {
			eptr->mode = KILL;
			return ;
		}
		PUT64BIT(eptr->chunkid,ptr);
		PUT8BIT(status,ptr);
		return;
	}
	stats_hlopr++;
	eptr->operation = READING;
	csserv_read_continue(eptr);
}

//writting

void csserv_write_init(csserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t ip;
	uint16_t port;
	uint8_t *ptr;
	uint8_t status;

	if (eptr->operation!=IDLE) {
		eptr->mode = KILL;
		return;
	}
	if (length<8+4 || (((length-(8+4))%(4+2))!=0)) {
		syslog(LOG_NOTICE,"CUTOCS_WRITE - wrong size (%"PRIu32"/12+N*6)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(eptr->chunkid,data);
	GET32BIT(eptr->version,data);
	status = check_chunk(eptr->chunkid,eptr->version);
	if (status!=STATUS_OK) {
		ptr = csserv_createpacket(eptr,CSTOCU_WRITE_STATUS,8+4+1);
		if (ptr==NULL) {
			eptr->mode=KILL;
			return;
		}
		PUT64BIT(eptr->chunkid,ptr);
		PUT32BIT(0,ptr);
		PUT8BIT(status,ptr);
		return;
	}
	if (length>(8+4)) {	// connect to another cs
		GET32BIT(ip,data);
		GET16BIT(port,data);
		eptr->operation = CONNECTING;
		eptr->chainleng = (length-(8+4+4+2));
		if (eptr->chainleng>0) {
			eptr->chain = (uint8_t*)malloc(eptr->chainleng);
			memcpy(eptr->chain,data,eptr->chainleng);
		} else {
			eptr->chain = NULL;
		}
		if (cstocsconn_newservconnection(ip,port,eptr)==0) {
			eptr->operation = WRITE_ERROR;
			ptr = csserv_createpacket(eptr,CSTOCU_WRITE_STATUS,8+4+1);
			if (ptr==NULL) {
				eptr->mode=KILL;
				return;
			}
			PUT64BIT(eptr->chunkid,ptr);
			PUT32BIT(0,ptr);
			PUT8BIT(ERROR_CANTCONNECT,ptr);
			return;
		}
	} else {	// you are the last one
		status = chunk_before_io(eptr->chunkid);
		if (status!=STATUS_OK) {
			ptr = csserv_createpacket(eptr,CSTOCU_WRITE_STATUS,8+4+1);
			if (ptr==NULL) {
				eptr->mode=KILL;
				return;
			}
			PUT64BIT(eptr->chunkid,ptr);
			PUT32BIT(0,ptr);
			PUT8BIT(status,ptr);
			return;
		}
		eptr->conn=NULL;
		stats_hlopw++;
		eptr->operation = WRITING;	//i'm last in chain
		ptr = csserv_createpacket(eptr,CSTOCU_WRITE_STATUS,8+4+1);
		if (ptr==NULL) {
			eptr->mode=KILL;
			return;
		}
		PUT64BIT(eptr->chunkid,ptr);
		PUT32BIT(0,ptr);
		PUT8BIT(STATUS_OK,ptr);
// createpacket(CSTOCU_WRITE_STATUS,STATUS_OK)
	}
}

void csserv_cstocs_connected(void *e,void *cptr) {
	csserventry *eptr = (csserventry *)e;
	uint8_t status;
	uint8_t *ptr;
	if (eptr->operation!=CONNECTING) {	// it should never happend
		eptr->mode = KILL;
		return;
	}
	eptr->conn = cptr;
	cstocsconn_sendwrite(eptr->conn,eptr->chunkid,eptr->version,eptr->chain,eptr->chainleng);
	free(eptr->chain);
	eptr->chain = NULL;
	status = chunk_before_io(eptr->chunkid);
	if (status!=STATUS_OK) {
		eptr->operation = WRITE_ERROR;
		ptr = csserv_createpacket(eptr,CSTOCU_WRITE_STATUS,8+4+1);
		if (ptr==NULL) {
			eptr->mode=KILL;
			return;
		}
		PUT64BIT(eptr->chunkid,ptr);
		PUT32BIT(0,ptr);
		PUT8BIT(status,ptr);
		if (eptr->conn!=NULL) {	// should always be true
			cstocsconn_delete(eptr->conn);
			eptr->conn=NULL;
		}
		return;
	}
	stats_hlopw++;
	eptr->operation = WRITING;
}

void csserv_write_data(csserventry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint16_t blocknum;
	uint16_t offset;
	uint32_t size;
	uint32_t crc;
	uint32_t writeid;
	uint8_t *ptr;
	uint8_t status;

	if (eptr->operation != WRITING) {
		return; // just ignore this packet
	}
	if (length<8+4+2+2+4+4) {
		syslog(LOG_NOTICE,"CUTOCS_WRITE_DATA - wrong size (%"PRIu32"/24+size)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET32BIT(writeid,data);
	GET16BIT(blocknum,data);
	GET16BIT(offset,data);
	GET32BIT(size,data);
	GET32BIT(crc,data);
	if (length!=8+4+2+2+4+4+size) {
		syslog(LOG_NOTICE,"CUTOCS_WRITE_DATA - wrong size (%"PRIu32"/24+%"PRIu32")",length,size);
		eptr->mode = KILL;
		return;
	}
	if (chunkid!=eptr->chunkid) {
		ptr = csserv_createpacket(eptr,CSTOCU_WRITE_STATUS,8+4+1);
		if (ptr==NULL) {
			eptr->mode=KILL;
			return;
		}
		PUT64BIT(chunkid,ptr);
		PUT32BIT(writeid,ptr);
		PUT8BIT(ERROR_WRONGCHUNKID,ptr);
		if (eptr->conn!=NULL) {
			cstocsconn_delete(eptr->conn);
			eptr->conn=NULL;
		}
		chunk_after_io(eptr->chunkid);
		eptr->operation = WRITE_ERROR;
		return;
	}
	status = write_block_to_chunk(chunkid,eptr->version,blocknum,data,offset,size,crc);
	if (status!=STATUS_OK) {
		ptr = csserv_createpacket(eptr,CSTOCU_WRITE_STATUS,8+4+1);
		if (ptr==NULL) {
			eptr->mode=KILL;
			return;
		}
		PUT64BIT(chunkid,ptr);
		PUT32BIT(writeid,ptr);
		PUT8BIT(status,ptr);
		if (eptr->conn!=NULL) {
			cstocsconn_delete(eptr->conn);
			eptr->conn=NULL;
		}
		chunk_after_io(eptr->chunkid);
		eptr->operation = WRITE_ERROR;
		return;
	}
	if (eptr->conn) {
		cstocsconn_sendwritedata(eptr->conn,chunkid,writeid,blocknum,offset,size,crc,data);
	} else {
		ptr = csserv_createpacket(eptr,CSTOCU_WRITE_STATUS,8+4+1);
		if (ptr==NULL) {
			eptr->mode=KILL;
			return;
		}
		PUT64BIT(chunkid,ptr);
		PUT32BIT(writeid,ptr);
		PUT8BIT(STATUS_OK,ptr);
	}
}

void csserv_cstocs_gotstatus(void *e,uint64_t chunkid,uint32_t writeid,uint8_t s) {
	uint8_t *ptr;
	csserventry *eptr = (csserventry *)e;
	if (eptr->operation!=WRITING) {
		return;
	}
	ptr = csserv_createpacket(eptr,CSTOCU_WRITE_STATUS,8+4+1);
	PUT64BIT(chunkid,ptr);
	PUT32BIT(writeid,ptr);
	PUT8BIT(s,ptr);
	if (s!=STATUS_OK) {
		if (eptr->conn) {
			cstocsconn_delete(eptr->conn);
			eptr->conn=NULL;
		}
		chunk_after_io(eptr->chunkid);
		eptr->operation = WRITE_ERROR;
	}
}

void csserv_cstocs_disconnected(void *e) {
	uint8_t *ptr;
	csserventry *eptr = (csserventry *)e;
	if (eptr->operation==CONNECTING) {
		free(eptr->chain);
		eptr->chain = NULL;
	}
	ptr = csserv_createpacket(eptr,CSTOCU_WRITE_STATUS,8+4+1);
	PUT64BIT(eptr->chunkid,ptr);
	PUT32BIT(0,ptr);
	if (eptr->operation==CONNECTING) {
		PUT8BIT(ERROR_CANTCONNECT,ptr);
	} else {
		PUT8BIT(ERROR_DISCONNECTED,ptr);
	}
	if (eptr->operation==WRITING) {
		chunk_after_io(eptr->chunkid);
	}
	eptr->operation = WRITE_ERROR;
}


void csserv_get_chunk_blocks(csserventry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t *ptr;
	uint8_t status;
	uint16_t blocks;
	
	if (length!=8+4) {
		syslog(LOG_NOTICE,"CSTOCS_GET_CHUNK_BLOCKS - wrong size (%"PRIu32"/12)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET32BIT(version,data);
	status = get_chunk_blocks(chunkid,version,&blocks);
	ptr = csserv_createpacket(eptr,CSTOCS_GET_CHUNK_BLOCKS_STATUS,8+4+2+1);
	if (ptr==NULL) {
		eptr->mode=KILL;
		return;
	}
	PUT64BIT(chunkid,ptr);
	PUT32BIT(version,ptr);
	PUT16BIT(blocks,ptr);
	PUT8BIT(status,ptr);
}

void csserv_chunk_checksum(csserventry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t *ptr;
	uint8_t status;
	uint32_t checksum;
	
	if (length!=8+4) {
		syslog(LOG_NOTICE,"ANTOCS_CHUNK_CHECKSUM - wrong size (%"PRIu32"/12)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET32BIT(version,data);
	status = get_chunk_checksum(chunkid,version,&checksum);
	if (status!=STATUS_OK) {
		ptr = csserv_createpacket(eptr,CSTOAN_CHUNK_CHECKSUM,8+4+1);
	} else {
		ptr = csserv_createpacket(eptr,CSTOAN_CHUNK_CHECKSUM,8+4+4);
	}
	if (ptr==NULL) {
		eptr->mode=KILL;
		return;
	}
	PUT64BIT(chunkid,ptr);
	PUT32BIT(version,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		PUT32BIT(checksum,ptr);
	}
}

void csserv_chunk_checksum_tab(csserventry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t *ptr;
	uint8_t status;
	uint8_t crctab[4096];
	
	if (length!=8+4) {
		syslog(LOG_NOTICE,"ANTOCS_CHUNK_CHECKSUM_TAB - wrong size (%"PRIu32"/12)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET32BIT(version,data);
	status = get_chunk_checksum_tab(chunkid,version,crctab);
	if (status!=STATUS_OK) {
		ptr = csserv_createpacket(eptr,CSTOAN_CHUNK_CHECKSUM_TAB,8+4+1);
	} else {
		ptr = csserv_createpacket(eptr,CSTOAN_CHUNK_CHECKSUM_TAB,8+4+4096);
	}
	if (ptr==NULL) {
		eptr->mode=KILL;
		return;
	}
	PUT64BIT(chunkid,ptr);
	PUT32BIT(version,ptr);
	if (status!=STATUS_OK) {
		PUT8BIT(status,ptr);
	} else {
		memcpy(ptr,crctab,4096);
	}
}

void csserv_hdd_list(csserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t l;
	uint8_t *ptr;

	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CUTOCS_HDD_LIST - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	l = hdd_diskinfo_size();
	ptr = csserv_createpacket(eptr,CSTOCU_HDD_LIST,l);
	if (ptr==NULL) {
		eptr->mode=KILL;
		return;
	}
	hdd_diskinfo_data(ptr);
}

void csserv_chart(csserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t chartid;
	uint8_t *ptr;
	uint32_t l;
	
	if (length!=4) {
		syslog(LOG_NOTICE,"CUTOAN_CHART - wrong size (%"PRIu32"/4)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(chartid,data);
	l = stats_gifsize(chartid);
	ptr = csserv_createpacket(eptr,ANTOCU_CHART,l);
	if (ptr==NULL) {
		eptr->mode=KILL;
		return;
	}
	if (l>0) {
		stats_makegif(ptr);
	}
}

void csserv_chart_data(csserventry *eptr,uint8_t *data,uint32_t length) {
	uint32_t chartid;
	uint8_t *ptr;
	uint32_t l;
	
	if (length!=4) {
		syslog(LOG_NOTICE,"CUTOAN_CHART_DATA - wrong size (%"PRIu32"/4)",length);
		eptr->mode = KILL;
		return;
	}
	GET32BIT(chartid,data);
	l = stats_datasize(chartid);
	ptr = csserv_createpacket(eptr,ANTOCU_CHART_DATA,l);
	if (ptr==NULL) {
		eptr->mode=KILL;
		return;
	}
	if (l>0) {
		stats_makedata(ptr,chartid);
	}
}

void csserv_outputcheck(csserventry *eptr) {
	if (eptr->operation==READING) {
		csserv_read_continue(eptr);
	}
}

void csserv_before_close(csserventry *eptr) {
	if (eptr->operation==CONNECTING || eptr->operation==WRITING) {
		if (eptr->conn) {
			cstocsconn_delete(eptr->conn);
			eptr->conn=NULL;
		}
	}
	if (eptr->operation==CONNECTING && eptr->chain) {
		free(eptr->chain);
	}
	if (eptr->operation==WRITING || eptr->operation==READING) {
		chunk_after_io(eptr->chunkid);
	}
}

void csserv_gotpacket(csserventry *eptr,uint32_t type,uint8_t *data,uint32_t length) {
	switch (type) {
		case ANTOAN_NOP:
			break;
		case CUTOCS_READ:
			csserv_read_init(eptr,data,length);
			break;
		case CUTOCS_WRITE:
			csserv_write_init(eptr,data,length);
			break;
		case CUTOCS_WRITE_DATA:
			csserv_write_data(eptr,data,length);
			break;
//		case CUTOCS_WRITE_DONE:
//			csserv_write_done(eptr,data,length);
//			break;
		case CSTOCS_GET_CHUNK_BLOCKS:
			csserv_get_chunk_blocks(eptr,data,length);
			break;
		case ANTOCS_CHUNK_CHECKSUM:
			csserv_chunk_checksum(eptr,data,length);
			break;
		case ANTOCS_CHUNK_CHECKSUM_TAB:
			csserv_chunk_checksum_tab(eptr,data,length);
			break;
		case CUTOCS_HDD_LIST:
			csserv_hdd_list(eptr,data,length);
			break;
		case CUTOAN_CHART:
			csserv_chart(eptr,data,length);
			break;
		case CUTOAN_CHART_DATA:
			csserv_chart_data(eptr,data,length);
			break;
		default:
			syslog(LOG_NOTICE,"got unknown message (type:%"PRIu32")",type);
			eptr->mode = KILL;
	}
}

void csserv_term(void) {
	csserventry *eptr,*eaptr;
	packetstruct *pptr,*paptr;

	syslog(LOG_INFO,"closing %s:%s",ListenHost,ListenPort);
	tcpclose(lsock);
	
	eptr = csservhead;
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
	csservhead=NULL;
}


int csserv_canread(csserventry *eptr) {
	if (eptr->operation==READING || eptr->operation==CONNECTING) {
		return 0;
	}
	if (eptr->operation==WRITING && eptr->conn!=NULL && cstocsconn_queueisfilled(eptr->conn)) {
		return 0;
	}
	return 1;
}

void csserv_read(csserventry *eptr) {
	int32_t i;
	uint32_t type,size;
	uint8_t *ptr;
	if (csserv_canread(eptr)==0) {
		return;
	}

	i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
	if (i==0) {
//		syslog(LOG_INFO,"connection lost");
		eptr->mode = KILL;
		return;
	}
	if (i<0) {
		syslog(LOG_INFO,"read error: %m");
		eptr->mode = KILL;
		return;
	}
	stats_bytesin+=i;
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
				syslog(LOG_WARNING,"packet too long (%"PRIu32"/%u)",size,MaxPacketSize);
				eptr->mode = KILL;
				return;
			}
			eptr->inputpacket.packet = malloc(size);
			if (eptr->inputpacket.packet==NULL) {
				syslog(LOG_WARNING,"out of memory");
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

		csserv_gotpacket(eptr,type,eptr->inputpacket.packet,size);

		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		eptr->inputpacket.packet=NULL;
		return;
	}
}

void csserv_write(csserventry *eptr) {
	packetstruct *pack;
	int32_t i;
	pack = eptr->outputhead;
	if (pack==NULL) {
		return;
	}
	i=write(eptr->sock,pack->startptr,pack->bytesleft);
	if (i<0) {
		syslog(LOG_INFO,"write error: %m");
		eptr->mode = KILL;
		return;
	}
	stats_bytesout+=i;
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
	csserv_outputcheck(eptr);
}

int csserv_desc(fd_set *rset,fd_set *wset) {
	int max=lsock;
	csserventry *eptr;
	int i;
	FD_SET(lsock,rset);
	for (eptr=csservhead ; eptr ; eptr=eptr->next) {
		i=eptr->sock;
		if (csserv_canread(eptr)) {
			FD_SET(i,rset);
			if (i>max) max=i;
		}
		if (eptr->outputhead!=NULL) {
			FD_SET(i,wset);
			if (i>max) max=i;
		}
	}
	return max;
}

void csserv_serve(fd_set *rset,fd_set *wset) {
	uint32_t now=main_time();
	csserventry *eptr,**kptr;
	packetstruct *pptr,*paptr;
	int ns;

	if (FD_ISSET(lsock,rset)) {
		ns=tcpaccept(lsock);
		if (ns<0) {
			syslog(LOG_INFO,"accept error: %m");
		} else {
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = malloc(sizeof(csserventry));
			eptr->next = csservhead;
			csservhead = eptr;
			eptr->sock = ns;
			eptr->mode = HEADER;
			eptr->activity = now;
			eptr->inputpacket.next = NULL;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;
			eptr->inputpacket.packet = NULL;
			eptr->outputhead = NULL;
			eptr->outputtail = &(eptr->outputhead);
			eptr->conn = NULL;
			eptr->operation=IDLE;
		}
	}
	for (eptr=csservhead ; eptr ; eptr=eptr->next) {
		if (FD_ISSET(eptr->sock,rset) && eptr->mode!=KILL) {
			eptr->activity = now;
			csserv_read(eptr);
		}
		if (FD_ISSET(eptr->sock,wset) && eptr->mode!=KILL) {
			eptr->activity = now;
			csserv_write(eptr);
		}
		if (eptr->activity+Timeout<now) {
			eptr->mode = KILL;
		}
	}
	kptr = &csservhead;
	while ((eptr=*kptr)) {
		if (eptr->mode == KILL) {
			csserv_before_close(eptr);
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

uint32_t csserv_getlistenip() {
	return mylistenip;
}

uint16_t csserv_getlistenport() {
	return mylistenport;
}

int csserv_init(void) {
	config_getnewstr("CSSERV_LISTEN_HOST","*",&ListenHost);
	config_getnewstr("CSSERV_LISTEN_PORT","9422",&ListenPort);
	config_getuint32("CSSERV_TIMEOUT",60,&Timeout);

	lsock = tcpsocket();
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	tcpaddrconvert(ListenHost,ListenPort,&mylistenip,&mylistenport);
	tcpnumlisten(lsock,mylistenip,mylistenport,5);
	if (lsock<0) {
		syslog(LOG_ERR,"listen error: %m");
		return -1;
	}
	syslog(LOG_NOTICE,"listen on %s:%s",ListenHost,ListenPort);

	csservhead = NULL;
	main_destructregister(csserv_term);
	main_selectregister(csserv_desc,csserv_serve);
	return 0;
}
