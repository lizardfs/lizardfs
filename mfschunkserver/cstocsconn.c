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
#include "cstocsconn.h"
#include "cfg.h"
#include "main.h"
#include "sockets.h"

#include "replicator.h"
#include "csserv.h"

#define MaxPacketSize 100000

enum {KILL,CONNECTING,HEADER,DATA};
enum {NONE,REPLICATOR,SERVER};

typedef struct packetstruct {
	struct packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t *packet;
} packetstruct;

typedef struct cstocsconnentry {
	uint8_t mode;
	int sock;
	time_t activity;
	uint8_t hdrbuff[8];
	packetstruct inputpacket;
	packetstruct *outputhead,**outputtail;

	int8_t type;			// replicator / server
	void *ptr;

	struct cstocsconnentry *next;
} cstocsconnentry;

static cstocsconnentry *cstocsconnhead=NULL;

static uint32_t stats_bytesout = 0;
static uint32_t stats_bytesin = 0;

// from config
static uint32_t Timeout;

void cstocsconn_stats(uint32_t *bin,uint32_t *bout) {
	*bin = stats_bytesin;
	*bout = stats_bytesout;
	stats_bytesin = 0;
	stats_bytesout = 0;
}

uint8_t* cstocsconn_createpacket(cstocsconnentry *eptr,uint32_t type,uint32_t size) {
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


void cstocsconn_replinit(void *e,uint64_t chunkid, uint32_t version) {
	cstocsconnentry *eptr = (cstocsconnentry*)e;
	uint8_t *ptr;
	if (eptr->type != REPLICATOR) {
		return;
	}
	ptr = cstocsconn_createpacket(eptr,CSTOCS_GET_CHUNK_BLOCKS,8+4);
	if (ptr==NULL) {
		eptr->mode = KILL;
	}
	PUT64BIT(chunkid,ptr);
	PUT32BIT(version,ptr);
}

void cstocsconn_got_chunk_blocks(cstocsconnentry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint32_t leng;
	uint16_t blocks;
	uint8_t status;
	uint8_t *ptr;
	if (eptr->type != REPLICATOR) {
		return;
	}
	if (length!=8+4+2+1) {
		syslog(LOG_NOTICE,"CSTOCS_GET_CHUNK_BLOCKS_STATUS - wrong size (%"PRIu32"/15)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET32BIT(version,data);
	GET16BIT(blocks,data);
	GET8BIT(status,data);
	//syslog(LOG_NOTICE,"got chunk length: %"PRIu64",%"PRIu32",%"PRIu32",%"PRIu8,chunkid,version,length,status);
	if (status!=STATUS_OK) {
		replicator_cstocs_gotstatus(eptr->ptr,chunkid,status);
		return;
	}
	if (blocks==0) {
		replicator_cstocs_gotstatus(eptr->ptr,chunkid,STATUS_OK);
		return;
	}
	leng = blocks*0x10000;
	ptr = cstocsconn_createpacket(eptr,CUTOCS_READ,8+4+4+4);
	if (ptr==NULL) {
		eptr->mode = KILL;
	}
	PUT64BIT(chunkid,ptr);
	PUT32BIT(version,ptr);
	PUT32BIT(0,ptr);			// offset = 0
    PUT32BIT(leng,ptr);
}

#if 0
void cstocsconn_readinit(void *e,uint64_t chunkid, uint32_t version/*, uint32_t offset, uint32_t size*/) {
	cstocsconnentry *eptr = (cstocsconnentry*)e;
	uint8_t *ptr;
	if (eptr->type != REPLICATOR) {
		return;
	}
	ptr = cstocsconn_createpacket(eptr,CUTOCS_READ,8+4+4+4);
	if (ptr==NULL) {
		eptr->mode = KILL;
	}
	PUT64BIT(chunkid,ptr);
	PUT32BIT(version,ptr);
	PUT32BIT(0,ptr);			//		PUT32BIT(offset,ptr);
    PUT32BIT(0x4000000,ptr);	//		PUT32BIT(size,ptr);
}
#endif

void cstocsconn_readdata(cstocsconnentry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint16_t blocknum;
	uint16_t offset;
	uint32_t size;
	uint32_t crc;
	if (eptr->type != REPLICATOR) {
		return;
	}
	if (length<8+2+2+4+4) {
		syslog(LOG_NOTICE,"CSTOCU_READ_DATA - wrong size (%"PRIu32"/20+size)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET16BIT(blocknum,data);
	GET16BIT(offset,data);
	GET32BIT(size,data);
	GET32BIT(crc,data);
	if (length!=8+2+2+4+4+size) {
		syslog(LOG_NOTICE,"CSTOCU_READ_DATA - wrong size (%"PRIu32"/20+%"PRIu32")",length,size);
		eptr->mode = KILL;
		return;
	}
	replicator_cstocs_gotdata(eptr->ptr,chunkid,blocknum,offset,size,crc,data);
}

void cstocsconn_readstatus(cstocsconnentry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t status;
	if (eptr->type != REPLICATOR) {
		return;
	}
	if (length!=8+1) {
		syslog(LOG_NOTICE,"CSTOCU_READ_STATUS - wrong size (%"PRIu32"/9)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET8BIT(status,data);
	replicator_cstocs_gotstatus(eptr->ptr,chunkid,status);
}

void cstocsconn_writestatus(cstocsconnentry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t writeid;
	uint8_t s;
//	syslog(LOG_NOTICE,"write-status");
	if (eptr->type != SERVER) {
		return;
	}
	if (length!=8+4+1) {
		syslog(LOG_NOTICE,"CSTOCU_WRITE_STATUS - wrong size (%"PRIu32"/13)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET32BIT(writeid,data);
	GET8BIT(s,data);
//	syslog(LOG_NOTICE,"%"PRIu64",%"PRIu8,chunkid,s);
	csserv_cstocs_gotstatus(eptr->ptr,chunkid,writeid,s);
}

void cstocsconn_sendwrite(void *e,uint64_t chunkid,uint32_t version,uint8_t *chain,uint32_t chainleng) {
	cstocsconnentry *eptr = (cstocsconnentry*)e;
	uint8_t *ptr;
	if (eptr->type != SERVER) {
		return;
	}
	ptr = cstocsconn_createpacket(eptr,CUTOCS_WRITE,8+4+chainleng);
	if (ptr==NULL) {
		eptr->mode = KILL;
	}
	PUT64BIT(chunkid,ptr);
	PUT32BIT(version,ptr);
	memcpy(ptr,chain,chainleng);
}

void cstocsconn_sendwritedata(void *e,uint64_t chunkid,uint32_t writeid,uint16_t blocknum,uint16_t offset,uint32_t size,uint32_t crc,uint8_t *data) {
	cstocsconnentry *eptr = (cstocsconnentry*)e;
	uint8_t *ptr;
	if (eptr->type != SERVER) {
		return;
	}
	ptr = cstocsconn_createpacket(eptr,CUTOCS_WRITE_DATA,8+4+2+2+4+4+size);
	if (ptr==NULL) {
		eptr->mode = KILL;
	}
	PUT64BIT(chunkid,ptr);
	PUT32BIT(writeid,ptr);
	PUT16BIT(blocknum,ptr);
	PUT16BIT(offset,ptr);
	PUT32BIT(size,ptr);
	PUT32BIT(crc,ptr);
	memcpy(ptr,data,size);
}

/*
void cstocsconn_sendwritedone(void *e,uint64_t chunkid) {
	cstocsconnentry *eptr = (cstocsconnentry*)e;
	uint8_t *ptr;
	if (eptr->type != SERVER) {
		return;
	}
	ptr = cstocsconn_createpacket(eptr,CUTOCS_WRITE_DONE,8);
	if (ptr==NULL) {
		eptr->mode = KILL;
	}
	PUT64BIT(chunkid,ptr);
}
*/

void cstocsconn_delete(void *e) {
	cstocsconnentry *eptr = (cstocsconnentry*)e;
	eptr->type = NONE;
	eptr->mode = KILL;
}

int cstocsconn_queueisfilled(void *e) {
	cstocsconnentry *eptr = (cstocsconnentry*)e;
	if (eptr->outputhead==NULL) {
		return 0;	// empty queue - can receive
	}
	if (eptr->outputhead->next==NULL) {
		return 0;	// one packet in queue - can receive
	}
	return 1;	// more than one packet - can't receive
}

void cstocsconn_gotpacket(cstocsconnentry *eptr,uint32_t type,uint8_t *data,uint32_t length) {
	switch (type) {
		case ANTOAN_NOP:
			break;
		case CSTOCS_GET_CHUNK_BLOCKS_STATUS:
			cstocsconn_got_chunk_blocks(eptr,data,length);
			break;
		case CSTOCU_READ_DATA:
			cstocsconn_readdata(eptr,data,length);
			break;
		case CSTOCU_READ_STATUS:
			cstocsconn_readstatus(eptr,data,length);
			break;
		case CSTOCU_WRITE_STATUS:
			cstocsconn_writestatus(eptr,data,length);
			break;
		default:
			syslog(LOG_NOTICE,"got unknown message (type:%"PRIu32")",type);
	}
}

void cstocsconn_term(void) {
	cstocsconnentry *eptr,*eaptr;
	packetstruct *pptr,*paptr;

	eptr = cstocsconnhead;
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
	cstocsconnhead=NULL;
}

void cstocsconn_connected(cstocsconnentry *eptr) {
	eptr->mode = HEADER;
	eptr->inputpacket.next = NULL;
	eptr->inputpacket.bytesleft = 8;
	eptr->inputpacket.startptr = eptr->hdrbuff;
	eptr->inputpacket.packet = NULL;
	eptr->outputhead = NULL;
	eptr->outputtail = &(eptr->outputhead);
	eptr->activity = main_time();
	// notify about succesfull connection
	if (eptr->type == REPLICATOR) {
		replicator_cstocs_connected(eptr->ptr,eptr);
	} else if (eptr->type == SERVER) {
		csserv_cstocs_connected(eptr->ptr,eptr);
	}
}

int cstocsconn_initconnect(cstocsconnentry *eptr,uint32_t ip,uint16_t port) {
	int status;
//	syslog(LOG_NOTICE,"connecting to: %08"PRIX32":%"PRIu16"",ip,port);
	eptr->sock=tcpsocket();
	if (eptr->sock<0) {
		syslog(LOG_WARNING,"create socket, error: %m");
		return -1;
	}
	if (tcpnonblock(eptr->sock)<0) {
		syslog(LOG_WARNING,"set nonblock, error: %m");
		tcpclose(eptr->sock);
		eptr->sock=-1;
		return -1;
	}
	status = tcpnumconnect(eptr->sock,ip,port);
	if (status<0) {
		syslog(LOG_WARNING,"connect failed, error: %m");
		tcpclose(eptr->sock);
		eptr->sock=-1;
		return -1;
	}
	if (status==0) {
//		syslog(LOG_NOTICE,"connected to ChunkServer immediately");
		cstocsconn_connected(eptr);
	} else {
		eptr->mode=CONNECTING;
//		syslog(LOG_NOTICE,"connecting ...");
	}
	return 0;
}

void cstocsconn_connecttest(cstocsconnentry *eptr) {
	int status;

	status = tcpgetstatus(eptr->sock);
	if (status) {
		syslog(LOG_WARNING,"connection failed, error: %m");
		tcpclose(eptr->sock);
		eptr->sock=-1;
		eptr->mode=KILL;
	} else {
//		syslog(LOG_NOTICE,"connected to ChunkServer");
		cstocsconn_connected(eptr);
	}
}

void cstocsconn_read(cstocsconnentry *eptr) {
	int32_t i;
	uint32_t type,size;
	uint8_t *ptr;
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

		cstocsconn_gotpacket(eptr,type,eptr->inputpacket.packet,size);

		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		eptr->inputpacket.packet=NULL;
		return;
	}
}

void cstocsconn_write(cstocsconnentry *eptr) {
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
}

int cstocsconn_desc(fd_set *rset,fd_set *wset) {
	int max=0;
	cstocsconnentry *eptr;
	int i;
	for (eptr=cstocsconnhead ; eptr ; eptr=eptr->next) {
		i=eptr->sock;
		if (eptr->mode==HEADER || eptr->mode==DATA) {
			FD_SET(i,rset);
			if (i>max) {
				max=i;
			}
		}
		if (eptr->mode==CONNECTING || ((eptr->mode==HEADER || eptr->mode==DATA) && eptr->outputhead!=NULL)) {
			FD_SET(i,wset);
			if (i>max) {
				max=i;
			}
		}
	}
	return max;
}

void cstocsconn_serve(fd_set *rset,fd_set *wset) {
	uint32_t now=main_time();
	cstocsconnentry *eptr,**kptr;
	packetstruct *pptr,*paptr;

	for (eptr=cstocsconnhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode==CONNECTING) {
			if (FD_ISSET(eptr->sock,wset)) {
				cstocsconn_connecttest(eptr);
			}
		} else {
			if ((eptr->mode==HEADER || eptr->mode==DATA) && FD_ISSET(eptr->sock,rset)) {
				eptr->activity = now;
				cstocsconn_read(eptr);
			}
			if ((eptr->mode==HEADER || eptr->mode==DATA) && FD_ISSET(eptr->sock,wset)) {
				eptr->activity = now;
				cstocsconn_write(eptr);
			}
			if ((eptr->mode==HEADER || eptr->mode==DATA) && eptr->activity+Timeout<now) {
				eptr->mode = KILL;
			}
		}
	}
	kptr = &cstocsconnhead;
	while ((eptr=*kptr)) {
		if (eptr->mode == KILL) {
			if (eptr->type == REPLICATOR) {
				replicator_cstocs_disconnected(eptr->ptr);
			} else if (eptr->type == SERVER) {
				csserv_cstocs_disconnected(eptr->ptr);
			}
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

uint8_t cstocsconn_newentry(uint32_t ip,uint16_t port,void *p,int8_t type) {
	cstocsconnentry *eptr;
	eptr = malloc(sizeof(cstocsconnentry));
	eptr->ptr = p;
	eptr->type = type;
	// init some pointers here because connect can change status to KILL before initialization of packet structutres.
	eptr->inputpacket.packet = NULL;
	eptr->outputhead = NULL;
	if (cstocsconn_initconnect(eptr,ip,port)<0) {
		free(eptr);
		return 0;
	} else {
		eptr->next = cstocsconnhead;
		cstocsconnhead = eptr;
	}
	return 1;
}

uint8_t cstocsconn_newservconnection(uint32_t ip,uint16_t port,void *p) {
	return cstocsconn_newentry(ip,port,p,SERVER);
}

uint8_t cstocsconn_newreplconnection(uint32_t ip,uint16_t port,void *p) {
	return cstocsconn_newentry(ip,port,p,REPLICATOR);
}

int cstocsconn_init(void) {
	config_getuint32("CSTOCS_TIMEOUT",60,&Timeout);

	cstocsconnhead = NULL;
	main_destructregister(cstocsconn_term);
	main_selectregister(cstocsconn_desc,cstocsconn_serve);
	return 0;
}
