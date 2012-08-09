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

#define BGJOBS 1
#define BGJOBSCNT 1000

#include <time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>

#include "MFSCommunication.h"

#include "datapack.h"
#include "csserv.h"
#include "cfg.h"
#include "main.h"
#include "sockets.h"
#include "hddspacemgr.h"
// #include "cstocsconn.h"
#include "charts.h"
#include "slogger.h"
#ifdef BGJOBS
#include "bgjobs.h"
#endif
#include "massert.h"

// connection timeout in seconds
#define CSSERV_TIMEOUT 5

#define CONNECT_RETRIES 10
#define CONNECT_TIMEOUT(cnt) (((cnt)%2)?(300000*(1<<((cnt)>>1))):(200000*(1<<((cnt)>>1))))

#define MaxPacketSize 100000

//csserventry.mode
enum {HEADER,DATA};
//csserventry.state
enum {IDLE,READ,WRITELAST,CONNECTING,WRITEINIT,WRITEFWD,WRITEFINISH,CLOSE,CLOSEWAIT,CLOSED};

#ifdef BGJOBS
typedef struct writestatus {
	uint32_t writeid;
	struct writestatus *next;
} writestatus;
#endif

typedef struct packetstruct {
	struct packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t *packet;
} packetstruct;

typedef struct csserventry {
	uint8_t state;
	uint8_t mode;
	uint8_t fwdmode;

	int sock;
	int fwdsock;			// forwarding socket for writing
	uint64_t connstart;		// 'connect' start time in usec (for timeout and retry)
	uint32_t fwdip;			// 'connect' IP
	uint16_t fwdport;		// 'connect' port number
	uint8_t connretrycnt;		// 'connect' retry counter
	int32_t pdescpos;
	int32_t fwdpdescpos;
	uint32_t activity;
	uint8_t hdrbuff[8];
	uint8_t fwdhdrbuff[8];
	packetstruct inputpacket;
	uint8_t *fwdstartptr;		// used for forwarding inputpacket data
	uint32_t fwdbytesleft;		// used for forwarding inputpacket data
	packetstruct fwdinputpacket;	// used for receiving status from fwdsocket
	uint8_t *fwdinitpacket;		// used only for write initialization
	packetstruct *outputhead,**outputtail;


#ifdef BGJOBS
	/* write */
	uint32_t wjobid;
	uint32_t wjobwriteid;
	writestatus *todolist;

	/* read */
	uint32_t rjobid;
	uint8_t todocnt;		// R (read finished + send finished)

	/* common for read and write but meaning is different !!! */
	void *rpacket;
	void *wpacket;
#endif

	uint8_t chunkisopen;
	uint64_t chunkid;		// R+W
	uint32_t version;		// R+W
	uint32_t offset;		// R
	uint32_t size;			// R
//	uint8_t *chain;			// W
//	uint32_t chainleng;		// W
//	void *conn;			// W
//	uint64_t wop_chunkid;		// W
//	uint32_t wop_version;		// W
//	uint32_t wop_newversion;	// W
//	uint64_t wop_copychunkid;	// W
//	uint32_t wop_copyversion;	// W
//	uint32_t wop_length;		// W
//	uint32_t blocknum;		// for read operation

	struct csserventry *next;
} csserventry;

static csserventry *csservhead=NULL;
static int lsock;
static int32_t lsockpdescpos;

#ifdef BGJOBS
static void *jpool;
static int jobfd;
static int32_t jobfdpdescpos;
#endif

static uint32_t mylistenip;
static uint16_t mylistenport;

static uint64_t stats_bytesin=0;
static uint64_t stats_bytesout=0;
static uint32_t stats_hlopr=0;
static uint32_t stats_hlopw=0;
static uint32_t stats_maxjobscnt=0;

// from config
static char *ListenHost;
static char *ListenPort;

void csserv_stats(uint64_t *bin,uint64_t *bout,uint32_t *hlopr,uint32_t *hlopw,uint32_t *maxjobscnt) {
	*bin = stats_bytesin;
	*bout = stats_bytesout;
	*hlopr = stats_hlopr;
	*hlopw = stats_hlopw;
	*maxjobscnt = stats_maxjobscnt;
	stats_bytesin = 0;
	stats_bytesout = 0;
	stats_hlopr = 0;
	stats_hlopw = 0;
	stats_maxjobscnt = 0;
}

void* csserv_create_detached_packet(uint32_t type,uint32_t size) {
	packetstruct *outpacket;
	uint8_t *ptr;
	uint32_t psize;

	outpacket=(packetstruct*)malloc(sizeof(packetstruct));
	passert(outpacket);
	psize = size+8;
	outpacket->packet=malloc(psize);
	passert(outpacket->packet);
	outpacket->bytesleft = psize;
	ptr = outpacket->packet;
	put32bit(&ptr,type);
	put32bit(&ptr,size);
	outpacket->startptr = (uint8_t*)(outpacket->packet);
	outpacket->next = NULL;
	return outpacket;
}

uint8_t* csserv_get_packet_data(void *packet) {
	packetstruct *outpacket = (packetstruct*)packet;
	return outpacket->packet+8;
}

void csserv_delete_packet(void *packet) {
	packetstruct *outpacket = (packetstruct*)packet;
	free(outpacket->packet);
	free(outpacket);
}

void csserv_attach_packet(csserventry *eptr,void *packet) {
	packetstruct *outpacket = (packetstruct*)packet;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
}

void* csserv_preserve_inputpacket(csserventry *eptr) {
	void* ret;
	ret = eptr->inputpacket.packet;
	eptr->inputpacket.packet = NULL;
	return ret;
}

void csserv_delete_preserved(void *p) {
	if (p) {
		free(p);
	}
}

uint8_t* csserv_create_attached_packet(csserventry *eptr,uint32_t type,uint32_t size) {
	packetstruct *outpacket;
	uint8_t *ptr;
	uint32_t psize;

	outpacket=(packetstruct*)malloc(sizeof(packetstruct));
	passert(outpacket);
	psize = size+8;
	outpacket->packet=malloc(psize);
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

// initialize connection to another CS
int csserv_initconnect(csserventry *eptr) {
	int status;
	eptr->fwdsock=tcpsocket();
	if (eptr->fwdsock<0) {
		mfs_errlog(LOG_WARNING,"create socket, error");
		return -1;
	}
	if (tcpnonblock(eptr->fwdsock)<0) {
		mfs_errlog(LOG_WARNING,"set nonblock, error");
		tcpclose(eptr->fwdsock);
		eptr->fwdsock=-1;
		return -1;
	}
	status = tcpnumconnect(eptr->fwdsock,eptr->fwdip,eptr->fwdport);
	if (status<0) {
		mfs_errlog(LOG_WARNING,"connect failed, error");
		tcpclose(eptr->fwdsock);
		eptr->fwdsock=-1;
		return -1;
	}
	if (status==0) { // connected immediately
//		syslog(LOG_NOTICE,"connected immediately");
		tcpnodelay(eptr->fwdsock);
		eptr->state=WRITEINIT;
	} else {
//		gettimeofday(&(eptr->conninittime),NULL);
//		syslog(LOG_NOTICE,"connecting ...");
		eptr->state=CONNECTING;
		eptr->connstart=main_utime();
	}
	return 0;
}

void csserv_retryconnect(csserventry *eptr) {
	uint8_t *ptr;
	tcpclose(eptr->fwdsock);
	eptr->fwdsock=-1;
	eptr->connretrycnt++;
	if (eptr->connretrycnt<CONNECT_RETRIES) {
		if (csserv_initconnect(eptr)<0) {
			ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
			put64bit(&ptr,eptr->chunkid);
			put32bit(&ptr,0);
			put8bit(&ptr,ERROR_CANTCONNECT);
			eptr->state = WRITEFINISH;
			return;
		}
	} else {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
		put64bit(&ptr,eptr->chunkid);
		put32bit(&ptr,0);
		put8bit(&ptr,ERROR_CANTCONNECT);
		eptr->state = WRITEFINISH;
		return;
	}
}

int csserv_makefwdpacket(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint32_t psize;
	psize = 12+length;
	eptr->fwdbytesleft = 8+psize;
	eptr->fwdinitpacket = malloc(eptr->fwdbytesleft);
	passert(eptr->fwdinitpacket);
	eptr->fwdstartptr = eptr->fwdinitpacket;
	if (eptr->fwdinitpacket==NULL) {
		return -1;
	}
	ptr = eptr->fwdinitpacket;
	put32bit(&ptr,CLTOCS_WRITE);
	put32bit(&ptr,psize);
	put64bit(&ptr,eptr->chunkid);
	put32bit(&ptr,eptr->version);
	if (length>0) {
		memcpy(ptr,data,length);
	}
	return 0;
}

#ifdef BGJOBS

void csserv_check_nextpacket(csserventry *eptr);

// common - delayed close
void csserv_delayed_close(uint8_t status,void *e) {
	csserventry *eptr = (csserventry*)e;
	if (eptr->wjobid>0 && eptr->wjobwriteid==0 && status==STATUS_OK) {	// this was job_open
		eptr->chunkisopen = 1;
	}
	if (eptr->chunkisopen) {
		job_close(jpool,NULL,NULL,eptr->chunkid);
		eptr->chunkisopen=0;
	}
	eptr->state = CLOSED;
}


// bg reading

void csserv_read_continue(csserventry *eptr);

void csserv_read_finished(uint8_t status,void *e) {
	csserventry *eptr = (csserventry*)e;
	uint8_t *ptr;
	eptr->rjobid=0;
	if (status==STATUS_OK) {
		eptr->todocnt--;
		if (eptr->todocnt==0) {
			csserv_read_continue(eptr);
		}
	} else {
		if (eptr->rpacket) {
			csserv_delete_packet(eptr->rpacket);
			eptr->rpacket = NULL;
		}
		ptr = csserv_create_attached_packet(eptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&ptr,eptr->chunkid);
		put8bit(&ptr,status);
		job_close(jpool,NULL,NULL,eptr->chunkid);
		eptr->chunkisopen = 0;
		eptr->state = IDLE;	// after sending status even if there was an error it's possible to receive new requests on the same connection
	}
}

void csserv_send_finished(csserventry *eptr) {
	eptr->todocnt--;
	if (eptr->todocnt==0) {
		csserv_read_continue(eptr);
	}
}

void csserv_read_continue(csserventry *eptr) {
	uint16_t blocknum;
	uint16_t blockoffset;
	uint32_t size;
	uint8_t *ptr;

	if (eptr->rpacket) {
		csserv_attach_packet(eptr,eptr->rpacket);
		eptr->rpacket=NULL;
		eptr->todocnt++;
	}
	if (eptr->size==0) {	// everything have been read
		ptr = csserv_create_attached_packet(eptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&ptr,eptr->chunkid);
		put8bit(&ptr,STATUS_OK);
		job_close(jpool,NULL,NULL,eptr->chunkid);
		eptr->chunkisopen = 0;
		eptr->state = IDLE;	// no error - do not disconnect - go direct to the IDLE state, ready for requests on the same connection
	} else {
		blocknum = (eptr->offset)>>MFSBLOCKBITS;
		blockoffset = (eptr->offset)&MFSBLOCKMASK;
		if (((eptr->offset+eptr->size-1)>>MFSBLOCKBITS) == blocknum) {	// last block
			size = eptr->size;
		} else {
			size = MFSBLOCKSIZE-blockoffset;
		}
		eptr->rpacket = csserv_create_detached_packet(CSTOCL_READ_DATA,8+2+2+4+4+size);
		ptr = csserv_get_packet_data(eptr->rpacket);
		put64bit(&ptr,eptr->chunkid);
		put16bit(&ptr,blocknum);
		put16bit(&ptr,blockoffset);
		put32bit(&ptr,size);
		eptr->rjobid = job_read(jpool,csserv_read_finished,eptr,eptr->chunkid,eptr->version,blocknum,ptr+4,blockoffset,size,ptr);
		if (eptr->rjobid==0) {
			eptr->state = CLOSE;
			return;
		}
		eptr->todocnt++;
		eptr->offset+=size;
		eptr->size-=size;
	}
}

void csserv_read_init(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t status;

	if (length!=8+4+4+4) {
		syslog(LOG_NOTICE,"CLTOCS_READ - wrong size (%"PRIu32"/20)",length);
		eptr->state = CLOSE;
		return;
	}
	eptr->chunkid = get64bit(&data);
	eptr->version = get32bit(&data);
	eptr->offset = get32bit(&data);
	eptr->size = get32bit(&data);
	status = hdd_check_version(eptr->chunkid,eptr->version);
	if (status!=STATUS_OK) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&ptr,eptr->chunkid);
		put8bit(&ptr,status);
		return;
	}
	if (eptr->size==0) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&ptr,eptr->chunkid);
		put8bit(&ptr,STATUS_OK);	// no bytes to read - just return STATUS_OK
		return;
	}
	if (eptr->size>MFSCHUNKSIZE) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&ptr,eptr->chunkid);
		put8bit(&ptr,ERROR_WRONGSIZE);
		return;
	}
	if (eptr->offset>=MFSCHUNKSIZE || eptr->offset+eptr->size>MFSCHUNKSIZE) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&ptr,eptr->chunkid);
		put8bit(&ptr,ERROR_WRONGOFFSET);
		return;
	}
	status = hdd_open(eptr->chunkid);
	if (status!=STATUS_OK) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&ptr,eptr->chunkid);
		put8bit(&ptr,status);
		return;
	}
	stats_hlopr++;
	eptr->chunkisopen = 1;
	eptr->state = READ;
	eptr->todocnt = 0;
	eptr->rjobid = 0;
	csserv_read_continue(eptr);
}

// bg writing

void csserv_write_finished(uint8_t status,void *e) {
	csserventry *eptr = (csserventry*)e;
	uint8_t *ptr;
	writestatus **wpptr,*wptr;
//	syslog(LOG_NOTICE,"write job finished (jobid:%"PRIu32",chunkid:%"PRIu64",writeid:%"PRIu32",status:%"PRIu8")",eptr->wjobid,eptr->chunkid,eptr->wjobwriteid,status);
	eptr->wjobid = 0;
	if (status!=STATUS_OK) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
		put64bit(&ptr,eptr->chunkid);
		put32bit(&ptr,eptr->wjobwriteid);
		put8bit(&ptr,status);
		eptr->state = WRITEFINISH;
		return;
	}
	if (eptr->wjobwriteid==0) {
		eptr->chunkisopen = 1;
	}
	if (eptr->state==WRITELAST) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
		put64bit(&ptr,eptr->chunkid);
		put32bit(&ptr,eptr->wjobwriteid);
		put8bit(&ptr,STATUS_OK);
	} else {
		wpptr = &(eptr->todolist);
		while ((wptr=*wpptr)) {
			if (wptr->writeid==eptr->wjobwriteid) { // found - it means that it was added by status_receive
				ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
				put64bit(&ptr,eptr->chunkid);
				put32bit(&ptr,eptr->wjobwriteid);
				put8bit(&ptr,STATUS_OK);
				*wpptr = wptr->next;
				free(wptr);
			} else {
				wpptr = &(wptr->next);
			}
		}
		// not found - so add it
		wptr = malloc(sizeof(writestatus));
		passert(wptr);
		wptr->writeid = eptr->wjobwriteid;
		wptr->next = eptr->todolist;
		eptr->todolist = wptr;
	}
	csserv_check_nextpacket(eptr);
}

void csserv_write_init(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t status;

	if (length<12 || ((length-12)%6)!=0) {
		syslog(LOG_NOTICE,"CLTOCS_WRITE - wrong size (%"PRIu32"/12+N*6)",length);
		eptr->state = CLOSE;
		return;
	}
	eptr->chunkid = get64bit(&data);
	eptr->version = get32bit(&data);
	status = hdd_check_version(eptr->chunkid,eptr->version);
	if (status!=STATUS_OK) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
		put64bit(&ptr,eptr->chunkid);
		put32bit(&ptr,0);
		put8bit(&ptr,status);
		eptr->state = WRITEFINISH;
		return;
	}

	if (length>(8+4)) {	// connect to another cs
		eptr->fwdip = get32bit(&data);
		eptr->fwdport = get16bit(&data);
		eptr->connretrycnt = 0;
		if (csserv_makefwdpacket(eptr,data,length-12-6)<0) {
			ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
			put64bit(&ptr,eptr->chunkid);
			put32bit(&ptr,0);
			put8bit(&ptr,ERROR_CANTCONNECT);
			eptr->state = WRITEFINISH;
			return;
		}
		if (csserv_initconnect(eptr)<0) {
			ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
			put64bit(&ptr,eptr->chunkid);
			put32bit(&ptr,0);
			put8bit(&ptr,ERROR_CANTCONNECT);
			eptr->state = WRITEFINISH;
			return;
		}
	} else {
		eptr->state = WRITELAST;
	}
	stats_hlopw++;

	eptr->wjobwriteid = 0;
	eptr->wjobid = job_open(jpool,csserv_write_finished,eptr,eptr->chunkid);
}

void csserv_write_data(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint16_t blocknum;
	uint16_t offset;
	uint32_t size;
	uint32_t writeid;
	uint8_t *ptr;

	if (length<8+4+2+2+4+4) {
		syslog(LOG_NOTICE,"CLTOCS_WRITE_DATA - wrong size (%"PRIu32"/24+size)",length);
		eptr->state = CLOSE;
		return;
	}
	chunkid = get64bit(&data);
	writeid = get32bit(&data);
	blocknum = get16bit(&data);
	offset = get16bit(&data);
	size = get32bit(&data);
	if (length!=8+4+2+2+4+4+size) {
		syslog(LOG_NOTICE,"CLTOCS_WRITE_DATA - wrong size (%"PRIu32"/24+%"PRIu32")",length,size);
		eptr->state = CLOSE;
		return;
	}
	if (chunkid!=eptr->chunkid) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
		put64bit(&ptr,chunkid);
		put32bit(&ptr,writeid);
		put8bit(&ptr,ERROR_WRONGCHUNKID);
		eptr->state = WRITEFINISH;
		return;
	}
	if (eptr->wpacket) {
		csserv_delete_preserved(eptr->wpacket);
	}
	eptr->wpacket = csserv_preserve_inputpacket(eptr);
	eptr->wjobwriteid = writeid;
	eptr->wjobid = job_write(jpool,csserv_write_finished,eptr,chunkid,eptr->version,blocknum,data+4,offset,size,data);
//	syslog(LOG_NOTICE,"add write job (jobid:%"PRIu32",chunkid:%"PRIu64",writeid:%"PRIu32")",eptr->wjobid,chunkid,eptr->wjobwriteid);
}

void csserv_write_status(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint64_t chunkid;
	uint32_t writeid;
	uint8_t status;
	writestatus **wpptr,*wptr;

	if (length!=8+4+1) {
		syslog(LOG_NOTICE,"CSTOCL_WRITE_STATUS - wrong size (%"PRIu32"/13)",length);
		eptr->state = CLOSE;
		return;
	}
	chunkid = get64bit(&data);
	writeid = get32bit(&data);
	status = get8bit(&data);

//	syslog(LOG_NOTICE,"received write status (chunkid:%"PRIu64",writeid:%"PRIu32",status:%"PRIu8")",chunkid,writeid,status);

	if (eptr->chunkid!=chunkid) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
		put64bit(&ptr,eptr->chunkid);
		put32bit(&ptr,0);
		put8bit(&ptr,ERROR_WRONGCHUNKID);
		eptr->state = WRITEFINISH;
		return;
	}
	if (status!=STATUS_OK) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
		put64bit(&ptr,eptr->chunkid);
		put32bit(&ptr,writeid);
		put8bit(&ptr,status);
		eptr->state = WRITEFINISH;
		return;
	}
	wpptr = &(eptr->todolist);
	while ((wptr=*wpptr)) {
		if (wptr->writeid==writeid) { // found - means it was added by write_finished
			ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
			put64bit(&ptr,chunkid);
			put32bit(&ptr,writeid);
			put8bit(&ptr,STATUS_OK);
			*wpptr = wptr->next;
			free(wptr);
			return;
		} else {
			wpptr = &(wptr->next);
		}
	}
	// if not found then add record
	wptr = malloc(sizeof(writestatus));
	passert(wptr);
	wptr->writeid = writeid;
	wptr->next = eptr->todolist;
	eptr->todolist = wptr;
}

void csserv_fwderror(csserventry *eptr) {
	uint8_t *ptr;
	ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
	put64bit(&ptr,eptr->chunkid);
	put32bit(&ptr,0);
	if (eptr->state==CONNECTING) {
		put8bit(&ptr,ERROR_CANTCONNECT);
	} else {
		put8bit(&ptr,ERROR_DISCONNECTED);
	}
	eptr->state = WRITEFINISH;
}

#endif
#if 0 /* not BGJOBS (#else) */

// fg reading

void csserv_read_continue(csserventry *eptr) {
	uint16_t blocknum;
	uint16_t blockoffset;
	uint32_t size;
	uint8_t *ptr;
	uint8_t status;
	void *packet;

	blocknum = (eptr->offset)>>MFSBLOCKBITS;
	blockoffset = (eptr->offset)&MFSBLOCKMASK;
	if ((eptr->offset+eptr->size-1)>>MFSBLOCKBITS == blocknum) {	// last block
		size = eptr->size;
		packet = csserv_create_detached_packet(CSTOCL_READ_DATA,8+2+2+4+4+size);
		ptr = csserv_get_packet_data(packet);
		put64bit(&ptr,eptr->chunkid);
		put16bit(&ptr,blocknum);
		put16bit(&ptr,blockoffset);
		put32bit(&ptr,size);
		status = hdd_read(eptr->chunkid,eptr->version,blocknum,ptr+4,blockoffset,size,ptr);
		if (status!=STATUS_OK) {
			csserv_delete_packet(packet);
		} else {
			csserv_attach_packet(eptr,packet);
		}
		ptr = csserv_create_attached_packet(eptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&ptr,eptr->chunkid);
		put8bit(&ptr,status);
		hdd_close(eptr->chunkid);
		eptr->chunkisopen = 0;
		eptr->state = IDLE;
	} else {
		size = MFSBLOCKSIZE-blockoffset;
		packet = csserv_create_detached_packet(CSTOCL_READ_DATA,8+2+2+4+4+size);
		ptr = csserv_get_packet_data(packet);
		put64bit(&ptr,eptr->chunkid);
		put16bit(&ptr,blocknum);
		put16bit(&ptr,blockoffset);
		put32bit(&ptr,size);
		status = hdd_read(eptr->chunkid,eptr->version,blocknum,ptr+4,blockoffset,size,ptr);
		if (status!=STATUS_OK) {
			csserv_delete_packet(packet);
			ptr = csserv_create_attached_packet(eptr,CSTOCL_READ_STATUS,8+1);
			put64bit(&ptr,eptr->chunkid);
			put8bit(&ptr,status);
			hdd_close(eptr->chunkid);
			eptr->chunkisopen = 0;
			eptr->state = IDLE;
		} else {
			csserv_attach_packet(eptr,packet);
			eptr->offset+=size;
			eptr->size-=size;
		}
	}
}

void csserv_read_init(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t status;

	if (length!=8+4+4+4) {
		syslog(LOG_NOTICE,"CLTOCS_READ - wrong size (%"PRIu32"/20)",length);
		eptr->state = CLOSE;
		return;
	}
	eptr->chunkid = get64bit(&data);
	eptr->version = get32bit(&data);
	eptr->offset = get32bit(&data);
	eptr->size = get32bit(&data);
	status = hdd_check_version(eptr->chunkid,eptr->version);
	if (status!=STATUS_OK) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&ptr,eptr->chunkid);
		put8bit(&ptr,status);
		return;
	}
	if (eptr->size==0) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&ptr,eptr->chunkid);
		put8bit(&ptr,STATUS_OK);
		return;
	}
	if (eptr->size>MFSCHUNKSIZE) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&ptr,eptr->chunkid);
		put8bit(&ptr,ERROR_WRONGSIZE);
		return;
	}
	if (eptr->offset>=MFSCHUNKSIZE || eptr->offset+eptr->size>MFSCHUNKSIZE) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&ptr,eptr->chunkid);
		put8bit(&ptr,ERROR_WRONGOFFSET);
		return;
	}
	status = hdd_open(eptr->chunkid);
	if (status!=STATUS_OK) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_READ_STATUS,8+1);
		put64bit(&ptr,eptr->chunkid);
		put8bit(&ptr,status);
		return;
	}
	stats_hlopr++;
	eptr->chunkisopen = 1;
	eptr->state = READ;
	csserv_read_continue(eptr);
}

// fg writing

void csserv_write_init(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t status,ver;
	uint64_t chunkid,copychunkid;
	uint32_t version,newversion,copyversion,leng;

	if (length<12 || ((length-12)%6)!=0) {
		syslog(LOG_NOTICE,"CLTOCS_WRITE - wrong size (%"PRIu32"/12+N*6)",length);
		eptr->state = CLOSE;
		return;
	}
	eptr->wop_chunkid=0;
	eptr->chunkid = get64bit(&data);
	eptr->version = get32bit(&data);
	status = hdd_check_version(eptr->chunkid,eptr->version);
	if (status!=STATUS_OK) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
		put64bit(&ptr,eptr->chunkid);
		put32bit(&ptr,0);
		put8bit(&ptr,status);
		eptr->state = WRITEFINISH;
		return;
	}
	if (length>(8+4)) {	// connect to another cs
		eptr->fwdip = get32bit(&data);
		eptr->fwdport = get16bit(&data);
		eptr->connretrycnt = 0;
		if (csserv_makefwdpacket(eptr,data,length-12-6)<0) {
			ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
			put64bit(&ptr,eptr->chunkid);
			put32bit(&ptr,0);
			put8bit(&ptr,ERROR_CANTCONNECT);
			eptr->state = WRITEFINISH;
			return;
		}
		if (csserv_initconnect(eptr)<0) {
			ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
			put64bit(&ptr,eptr->chunkid);
			put32bit(&ptr,0);
			put8bit(&ptr,ERROR_CANTCONNECT);
			eptr->state = WRITEFINISH;
			return;
		}
		status = hdd_open(eptr->chunkid);
		if (status!=STATUS_OK) {
			ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
			put64bit(&ptr,eptr->chunkid);
			put32bit(&ptr,0);
			put8bit(&ptr,status);
			eptr->state = WRITEFINISH;
			return;
		}
		stats_hlopw++;
		eptr->chunkisopen = 1;
	} else {	// you are the last one
		status = hdd_open(eptr->chunkid);
		if (status!=STATUS_OK) {
			ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
			put64bit(&ptr,eptr->chunkid);
			put32bit(&ptr,0);
			put8bit(&ptr,status);
			eptr->state = WRITEFINISH;
			return;
		}
		stats_hlopw++;
		eptr->chunkisopen = 1;
		eptr->state = WRITELAST;	//i'm last in the chain
		ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
		put64bit(&ptr,eptr->chunkid);
		put32bit(&ptr,0);
		put8bit(&ptr,STATUS_OK);
// createpacket(CSTOCL_WRITE_STATUS,STATUS_OK)
	}
}

void csserv_write_data(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint16_t blocknum;
	uint16_t offset;
	uint32_t size;
	uint32_t writeid;
	uint8_t *ptr;
	uint8_t status;

	if (length<8+4+2+2+4+4) {
		syslog(LOG_NOTICE,"CLTOCS_WRITE_DATA - wrong size (%"PRIu32"/24+size)",length);
		eptr->state = CLOSE;
		return;
	}
	chunkid = get64bit(&data);
	writeid = get32bit(&data);
	blocknum = get16bit(&data);
	offset = get16bit(&data);
	size = get32bit(&data);
	if (length!=8+4+2+2+4+4+size) {
		syslog(LOG_NOTICE,"CLTOCS_WRITE_DATA - wrong size (%"PRIu32"/24+%"PRIu32")",length,size);
		eptr->state = CLOSE;
		return;
	}
	if (chunkid!=eptr->chunkid) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
		put64bit(&ptr,chunkid);
		put32bit(&ptr,writeid);
		put8bit(&ptr,ERROR_WRONGCHUNKID);
		eptr->state = WRITEFINISH;
		return;
	}
	status = hdd_write(chunkid,eptr->version,blocknum,data+4,offset,size,data);
	if (status!=STATUS_OK) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
		put64bit(&ptr,chunkid);
		put32bit(&ptr,writeid);
		put8bit(&ptr,status);
		eptr->state = WRITEFINISH;
		return;
	}
	if (eptr->state==WRITELAST) {
		ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
		put64bit(&ptr,chunkid);
		put32bit(&ptr,writeid);
		put8bit(&ptr,STATUS_OK);
	}
}

void csserv_write_status(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint64_t chunkid;
	uint32_t writeid;
	uint8_t status;

	if (length!=8+4+1) {
		syslog(LOG_NOTICE,"CSTOCL_WRITE_STATUS - wrong size (%"PRIu32"/13)",length);
		eptr->state = CLOSE;
		return;
	}
	chunkid = get64bit(&data);
	writeid = get32bit(&data);
	status = get8bit(&data);

	ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
	put64bit(&ptr,chunkid);
	put32bit(&ptr,writeid);
	put8bit(&ptr,status);
	if (status!=STATUS_OK) {
		eptr->state = WRITEFINISH;
	}
}

void csserv_fwderror(csserventry *eptr) {
	uint8_t *ptr;
	ptr = csserv_create_attached_packet(eptr,CSTOCL_WRITE_STATUS,8+4+1);
	put64bit(&ptr,eptr->chunkid);
	put32bit(&ptr,0);
	if (eptr->state==CONNECTING) {
		put8bit(&ptr,ERROR_CANTCONNECT);
	} else {
		put8bit(&ptr,ERROR_DISCONNECTED);
	}
	eptr->state = WRITEFINISH;
}

#endif /* BGJOBS */

/* IDLE operations */

void csserv_get_chunk_blocks(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t *ptr;
	uint8_t status;
	uint16_t blocks;

	if (length!=8+4) {
		syslog(LOG_NOTICE,"CSTOCS_GET_CHUNK_BLOCKS - wrong size (%"PRIu32"/12)",length);
		eptr->state = CLOSE;
		return;
	}
	chunkid = get64bit(&data);
	version = get32bit(&data);
	status = hdd_get_blocks(chunkid,version,&blocks);
	ptr = csserv_create_attached_packet(eptr,CSTOCS_GET_CHUNK_BLOCKS_STATUS,8+4+2+1);
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	put16bit(&ptr,blocks);
	put8bit(&ptr,status);
}

void csserv_chunk_checksum(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t *ptr;
	uint8_t status;
	uint32_t checksum;

	if (length!=8+4) {
		syslog(LOG_NOTICE,"ANTOCS_CHUNK_CHECKSUM - wrong size (%"PRIu32"/12)",length);
		eptr->state = CLOSE;
		return;
	}
	chunkid = get64bit(&data);
	version = get32bit(&data);
	status = hdd_get_checksum(chunkid,version,&checksum);
	if (status!=STATUS_OK) {
		ptr = csserv_create_attached_packet(eptr,CSTOAN_CHUNK_CHECKSUM,8+4+1);
	} else {
		ptr = csserv_create_attached_packet(eptr,CSTOAN_CHUNK_CHECKSUM,8+4+4);
	}
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,checksum);
	}
}

void csserv_chunk_checksum_tab(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t *ptr;
	uint8_t status;
	uint8_t crctab[4096];

	if (length!=8+4) {
		syslog(LOG_NOTICE,"ANTOCS_CHUNK_CHECKSUM_TAB - wrong size (%"PRIu32"/12)",length);
		eptr->state = CLOSE;
		return;
	}
	chunkid = get64bit(&data);
	version = get32bit(&data);
	status = hdd_get_checksum_tab(chunkid,version,crctab);
	if (status!=STATUS_OK) {
		ptr = csserv_create_attached_packet(eptr,CSTOAN_CHUNK_CHECKSUM_TAB,8+4+1);
	} else {
		ptr = csserv_create_attached_packet(eptr,CSTOAN_CHUNK_CHECKSUM_TAB,8+4+4096);
	}
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	if (status!=STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		memcpy(ptr,crctab,4096);
	}
}

void csserv_hdd_list_v1(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t l;
	uint8_t *ptr;

	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CLTOCS_HDD_LIST(1) - wrong size (%"PRIu32"/0)",length);
		eptr->state = CLOSE;
		return;
	}
	l = hdd_diskinfo_v1_size();	// lock
	ptr = csserv_create_attached_packet(eptr,CSTOCL_HDD_LIST_V1,l);
	hdd_diskinfo_v1_data(ptr);	// unlock
}

void csserv_hdd_list_v2(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t l;
	uint8_t *ptr;

	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CLTOCS_HDD_LIST(2) - wrong size (%"PRIu32"/0)",length);
		eptr->state = CLOSE;
		return;
	}
	l = hdd_diskinfo_v2_size();	// lock
	ptr = csserv_create_attached_packet(eptr,CSTOCL_HDD_LIST_V2,l);
	hdd_diskinfo_v2_data(ptr);	// unlock
}

void csserv_chart(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t chartid;
	uint8_t *ptr;
	uint32_t l;

	if (length!=4) {
		syslog(LOG_NOTICE,"CLTOAN_CHART - wrong size (%"PRIu32"/4)",length);
		eptr->state = CLOSE;
		return;
	}
	chartid = get32bit(&data);
	l = charts_make_png(chartid);
	ptr = csserv_create_attached_packet(eptr,ANTOCL_CHART,l);
	if (l>0) {
		charts_get_png(ptr);
	}
}

void csserv_chart_data(csserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t chartid;
	uint8_t *ptr;
	uint32_t l;

	if (length!=4) {
		syslog(LOG_NOTICE,"CLTOAN_CHART_DATA - wrong size (%"PRIu32"/4)",length);
		eptr->state = CLOSE;
		return;
	}
	chartid = get32bit(&data);
	l = charts_datasize(chartid);
	ptr = csserv_create_attached_packet(eptr,ANTOCL_CHART_DATA,l);
	if (l>0) {
		charts_makedata(ptr,chartid);
	}
}


void csserv_outputcheck(csserventry *eptr) {
	if (eptr->state==READ) {
#ifdef BGJOBS
		csserv_send_finished(eptr);
#else /* BGJOBS */
		csserv_read_continue(eptr);
#endif
	}
}

void csserv_close(csserventry *eptr) {
#ifdef BGJOBS
	if (eptr->rjobid>0) {
		job_pool_disable_job(jpool,eptr->rjobid);
		job_pool_change_callback(jpool,eptr->rjobid,csserv_delayed_close,eptr);
		eptr->state = CLOSEWAIT;
	} else if (eptr->wjobid>0) {
		job_pool_disable_job(jpool,eptr->wjobid);
		job_pool_change_callback(jpool,eptr->wjobid,csserv_delayed_close,eptr);
		eptr->state = CLOSEWAIT;
	} else {
		if (eptr->chunkisopen) {
			job_close(jpool,NULL,NULL,eptr->chunkid);
			eptr->chunkisopen=0;
		}
		eptr->state = CLOSED;
	}
#else /* BGJOBS */
	if (eptr->chunkisopen) {
		hdd_close(eptr->chunkid);
		eptr->chunkisopen=0;
	}
	eptr->state = CLOSED;
#endif /* BGJOBS */
}

void csserv_gotpacket(csserventry *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
//	syslog(LOG_NOTICE,"packet %u:%u",type,length);
	if (type==ANTOAN_NOP) {
		return;
	}
	if (type==ANTOAN_UNKNOWN_COMMAND) { // for future use
		return;
	}
	if (type==ANTOAN_BAD_COMMAND_SIZE) { // for future use
		return;
	}
	if (eptr->state==IDLE) {
		switch (type) {
		case CLTOCS_READ:
			csserv_read_init(eptr,data,length);
			break;
		case CLTOCS_WRITE:
			csserv_write_init(eptr,data,length);
			break;
//		case CLTOCS_WRITE_DATA:
//			csserv_write_data(eptr,data,length);
//			break;
//		case CLTOCS_WRITE_DONE:
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
		case CLTOCS_HDD_LIST_V1:
			csserv_hdd_list_v1(eptr,data,length);
			break;
		case CLTOCS_HDD_LIST_V2:
			csserv_hdd_list_v2(eptr,data,length);
			break;
		case CLTOAN_CHART:
			csserv_chart(eptr,data,length);
			break;
		case CLTOAN_CHART_DATA:
			csserv_chart_data(eptr,data,length);
			break;
		default:
			syslog(LOG_NOTICE,"got unknown message (type:%"PRIu32")",type);
			eptr->state = CLOSE;
		}
	} else if (eptr->state==WRITELAST) {
		if (type==CLTOCS_WRITE_DATA) {
			csserv_write_data(eptr,data,length);
		} else {
			syslog(LOG_NOTICE,"got unknown message (type:%"PRIu32")",type);
			eptr->state = CLOSE;
		}
	} else if (eptr->state==WRITEFWD) {
		switch (type) {
		case CLTOCS_WRITE_DATA:
			csserv_write_data(eptr,data,length);
			break;
		case CSTOCL_WRITE_STATUS:
			csserv_write_status(eptr,data,length);
			break;
		default:
			syslog(LOG_NOTICE,"got unknown message (type:%"PRIu32")",type);
			eptr->state = CLOSE;
		}
	} else if (eptr->state==WRITEFINISH) {
		if (type==CLTOCS_WRITE_DATA) {
			return;
		} else {
			syslog(LOG_NOTICE,"got unknown message (type:%"PRIu32")",type);
			eptr->state = CLOSE;
		}
	} else {
		syslog(LOG_NOTICE,"got unknown message (type:%"PRIu32")",type);
		eptr->state = CLOSE;
	}
}

void csserv_term(void) {
	csserventry *eptr,*eaptr;
	packetstruct *pptr,*paptr;
#ifdef BGJOBS
	writestatus *wptr,*waptr;
#endif

	syslog(LOG_NOTICE,"closing %s:%s",ListenHost,ListenPort);
	tcpclose(lsock);

#ifdef BGJOBS
	job_pool_delete(jpool);
#endif

	eptr = csservhead;
	while (eptr) {
		if (eptr->chunkisopen) {
			hdd_close(eptr->chunkid);
		}
		tcpclose(eptr->sock);
		if (eptr->fwdsock>=0) {
			tcpclose(eptr->fwdsock);
		}
		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		if (eptr->fwdinputpacket.packet) {
			free(eptr->fwdinputpacket.packet);
		}
		if (eptr->fwdinitpacket) {
			free(eptr->fwdinitpacket);
		}
#ifdef BGJOBS
		wptr = eptr->todolist;
		while (wptr) {
			waptr = wptr;
			wptr = wptr->next;
			free(waptr);
		}
#endif
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
	free(ListenHost);
	free(ListenPort);
}

void csserv_check_nextpacket(csserventry *eptr) {
	uint32_t type,size;
	const uint8_t *ptr;
	if (eptr->state==WRITEFWD) {
		if (eptr->mode==DATA && eptr->inputpacket.bytesleft==0 && eptr->fwdbytesleft==0) {
			ptr = eptr->hdrbuff;
			type = get32bit(&ptr);
			size = get32bit(&ptr);

			eptr->mode = HEADER;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;

			csserv_gotpacket(eptr,type,eptr->inputpacket.packet+8,size);

			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet=NULL;
		}
	} else {
		if (eptr->mode==DATA && eptr->inputpacket.bytesleft==0) {
			ptr = eptr->hdrbuff;
			type = get32bit(&ptr);
			size = get32bit(&ptr);

			eptr->mode = HEADER;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;

			csserv_gotpacket(eptr,type,eptr->inputpacket.packet,size);

			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet=NULL;
		}
	}
}

void csserv_fwdconnected(csserventry *eptr) {
	int status;
	status = tcpgetstatus(eptr->fwdsock);
	if (status) {
		mfs_errlog_silent(LOG_WARNING,"connection failed, error");
		csserv_fwderror(eptr);
		return;
	}
	tcpnodelay(eptr->fwdsock);
	eptr->state=WRITEINIT;
}

void csserv_fwdread(csserventry *eptr) {
	int32_t i;
	uint32_t type,size;
	const uint8_t *ptr;
	if (eptr->fwdmode==HEADER) {
		i=read(eptr->fwdsock,eptr->fwdinputpacket.startptr,eptr->fwdinputpacket.bytesleft);
		if (i==0) {
//			syslog(LOG_NOTICE,"(fwdread) connection closed");
			csserv_fwderror(eptr);
			return;
		}
		if (i<0) {
			if (errno!=EAGAIN) {
				mfs_errlog_silent(LOG_NOTICE,"(fwdread) read error");
				csserv_fwderror(eptr);
			}
			return;
		}
		stats_bytesin+=i;
		eptr->fwdinputpacket.startptr+=i;
		eptr->fwdinputpacket.bytesleft-=i;
		if (eptr->fwdinputpacket.bytesleft>0) {
			return;
		}
		ptr = eptr->fwdhdrbuff+4;
		size = get32bit(&ptr);
		if (size>MaxPacketSize) {
			syslog(LOG_WARNING,"(fwdread) packet too long (%"PRIu32"/%u)",size,MaxPacketSize);
			csserv_fwderror(eptr);
			return;
		}
		if (size>0) {
			eptr->fwdinputpacket.packet = malloc(size);
			passert(eptr->fwdinputpacket.packet);
			eptr->fwdinputpacket.startptr = eptr->fwdinputpacket.packet;
		}
		eptr->fwdinputpacket.bytesleft = size;
		eptr->fwdmode = DATA;
	}
	if (eptr->fwdmode==DATA) {
		if (eptr->fwdinputpacket.bytesleft>0) {
			i=read(eptr->fwdsock,eptr->fwdinputpacket.startptr,eptr->fwdinputpacket.bytesleft);
			if (i==0) {
//				syslog(LOG_NOTICE,"(fwdread) connection closed");
				csserv_fwderror(eptr);
				return;
			}
			if (i<0) {
				if (errno!=EAGAIN) {
					mfs_errlog_silent(LOG_NOTICE,"(fwdread) read error");
					csserv_fwderror(eptr);
				}
				return;
			}
			stats_bytesin+=i;
			eptr->fwdinputpacket.startptr+=i;
			eptr->fwdinputpacket.bytesleft-=i;
			if (eptr->fwdinputpacket.bytesleft>0) {
				return;
			}
		}
		ptr = eptr->fwdhdrbuff;
		type = get32bit(&ptr);
		size = get32bit(&ptr);

		eptr->fwdmode=HEADER;
		eptr->fwdinputpacket.bytesleft = 8;
		eptr->fwdinputpacket.startptr = eptr->fwdhdrbuff;

		csserv_gotpacket(eptr,type,eptr->fwdinputpacket.packet,size);

		if (eptr->fwdinputpacket.packet) {
			free(eptr->fwdinputpacket.packet);
		}
		eptr->fwdinputpacket.packet=NULL;
	}
}

void csserv_fwdwrite(csserventry *eptr) {
	int32_t i;
	if (eptr->fwdbytesleft>0) {
		i=write(eptr->fwdsock,eptr->fwdstartptr,eptr->fwdbytesleft);
		if (i==0) {
//			syslog(LOG_NOTICE,"(fwdwrite) connection closed");
			csserv_fwderror(eptr);
			return;
		}
		if (i<0) {
			if (errno!=EAGAIN) {
				mfs_errlog_silent(LOG_NOTICE,"(fwdwrite) write error");
				csserv_fwderror(eptr);
			}
			return;
		}
		stats_bytesout+=i;
		eptr->fwdstartptr+=i;
		eptr->fwdbytesleft-=i;
	}
	if (eptr->fwdbytesleft==0) {
		free(eptr->fwdinitpacket);
		eptr->fwdinitpacket = NULL;
		eptr->fwdstartptr = NULL;
//		eptr->fwdbytesleft = 0;
		eptr->fwdmode = HEADER;
		eptr->fwdinputpacket.bytesleft = 8;
		eptr->fwdinputpacket.startptr = eptr->fwdhdrbuff;
		eptr->fwdinputpacket.packet = NULL;
		eptr->state = WRITEFWD;
	}
}

void csserv_forward(csserventry *eptr) {
	int32_t i;
	uint32_t type,size;
	const uint8_t *ptr;
	if (eptr->mode==HEADER) {
		i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
		if (i==0) {
//			syslog(LOG_NOTICE,"(forward) connection closed");
			eptr->state = CLOSE;
			return;
		}
		if (i<0) {
			if (errno!=EAGAIN) {
				mfs_errlog_silent(LOG_NOTICE,"(forward) read error");
				eptr->state = CLOSE;
			}
			return;
		}
		stats_bytesin+=i;
		eptr->inputpacket.startptr+=i;
		eptr->inputpacket.bytesleft-=i;
		if (eptr->inputpacket.bytesleft>0) {
			return;
		}
		ptr = eptr->hdrbuff+4;
		size = get32bit(&ptr);
		if (size>MaxPacketSize) {
			syslog(LOG_WARNING,"(forward) packet too long (%"PRIu32"/%u)",size,MaxPacketSize);
			eptr->state = CLOSE;
			return;
		}
		eptr->inputpacket.packet = malloc(size+8);
		passert(eptr->inputpacket.packet);
		memcpy(eptr->inputpacket.packet,eptr->hdrbuff,8);
		eptr->inputpacket.bytesleft = size;
		eptr->inputpacket.startptr = eptr->inputpacket.packet+8;
		eptr->fwdbytesleft = 8;
		eptr->fwdstartptr = eptr->inputpacket.packet;
		eptr->mode = DATA;
	}
	if (eptr->inputpacket.bytesleft>0) {
		i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
		if (i==0) {
//			syslog(LOG_NOTICE,"(forward) connection closed");
			eptr->state = CLOSE;
			return;
		}
		if (i<0) {
			if (errno!=EAGAIN) {
				mfs_errlog_silent(LOG_NOTICE,"(forward) read error: %s");
				eptr->state = CLOSE;
			}
			return;
		}
		stats_bytesin+=i;
		eptr->inputpacket.startptr+=i;
		eptr->inputpacket.bytesleft-=i;
		eptr->fwdbytesleft+=i;
	}
	if (eptr->fwdbytesleft>0) {
		i=write(eptr->fwdsock,eptr->fwdstartptr,eptr->fwdbytesleft);
		if (i==0) {
//			syslog(LOG_NOTICE,"(forward) connection closed");
			csserv_fwderror(eptr);
			return;
		}
		if (i<0) {
			if (errno!=EAGAIN) {
				mfs_errlog_silent(LOG_NOTICE,"(forward) write error: %s");
				csserv_fwderror(eptr);
			}
			return;
		}
		stats_bytesout+=i;
		eptr->fwdstartptr+=i;
		eptr->fwdbytesleft-=i;
	}
#ifdef BGJOBS
	if (eptr->inputpacket.bytesleft==0 && eptr->fwdbytesleft==0 && eptr->wjobid==0) {
#else
	if (eptr->inputpacket.bytesleft==0 && eptr->fwdbytesleft==0) {
#endif
		ptr = eptr->hdrbuff;
		type = get32bit(&ptr);
		size = get32bit(&ptr);

		eptr->mode = HEADER;
		eptr->inputpacket.bytesleft = 8;
		eptr->inputpacket.startptr = eptr->hdrbuff;

		csserv_gotpacket(eptr,type,eptr->inputpacket.packet+8,size);

		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		eptr->inputpacket.packet=NULL;
	}
}

void csserv_read(csserventry *eptr) {
	int32_t i;
	uint32_t type,size;
	const uint8_t *ptr;

	if (eptr->mode == HEADER) {
		i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
		if (i==0) {
//			syslog(LOG_NOTICE,"(read) connection closed");
			eptr->state = CLOSE;
			return;
		}
		if (i<0) {
			if (errno!=EAGAIN) {
				mfs_errlog_silent(LOG_NOTICE,"(read) read error");
				eptr->state = CLOSE;
			}
			return;
		}
		stats_bytesin+=i;
		eptr->inputpacket.startptr+=i;
		eptr->inputpacket.bytesleft-=i;

		if (eptr->inputpacket.bytesleft>0) {
			return;
		}

		ptr = eptr->hdrbuff+4;
		size = get32bit(&ptr);

		if (size>0) {
			if (size>MaxPacketSize) {
				syslog(LOG_WARNING,"(read) packet too long (%"PRIu32"/%u)",size,MaxPacketSize);
				eptr->state = CLOSE;
				return;
			}
			eptr->inputpacket.packet = malloc(size);
			passert(eptr->inputpacket.packet);
			eptr->inputpacket.startptr = eptr->inputpacket.packet;
		}
		eptr->inputpacket.bytesleft = size;
		eptr->mode = DATA;
	}
	if (eptr->mode == DATA) {
		if (eptr->inputpacket.bytesleft>0) {
			i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
			if (i==0) {
//				syslog(LOG_NOTICE,"(read) connection closed");
				eptr->state = CLOSE;
				return;
			}
			if (i<0) {
				if (errno!=EAGAIN) {
					mfs_errlog_silent(LOG_NOTICE,"(read) read error");
					eptr->state = CLOSE;
				}
				return;
			}
			stats_bytesin+=i;
			eptr->inputpacket.startptr+=i;
			eptr->inputpacket.bytesleft-=i;

			if (eptr->inputpacket.bytesleft>0) {
				return;
			}
		}
#ifdef BGJOBS
		if (eptr->wjobid==0) {
#endif
		ptr = eptr->hdrbuff;
		type = get32bit(&ptr);
		size = get32bit(&ptr);

		eptr->mode = HEADER;
		eptr->inputpacket.bytesleft = 8;
		eptr->inputpacket.startptr = eptr->hdrbuff;

		csserv_gotpacket(eptr,type,eptr->inputpacket.packet,size);

		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		eptr->inputpacket.packet=NULL;
#ifdef BGJOBS
		}
#endif
	}
}

void csserv_write(csserventry *eptr) {
	packetstruct *pack;
	int32_t i;
	for (;;) {
		pack = eptr->outputhead;
		if (pack==NULL) {
			return;
		}
		i=write(eptr->sock,pack->startptr,pack->bytesleft);
		if (i==0) {
//			syslog(LOG_NOTICE,"(write) connection closed");
			eptr->state = CLOSE;
			return;
		}
		if (i<0) {
			if (errno!=EAGAIN) {
				mfs_errlog_silent(LOG_NOTICE,"(write) write error");
				eptr->state = CLOSE;
			}
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
}

void csserv_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
//	int max=lsock;
	csserventry *eptr;
//	int i;
	pdesc[pos].fd = lsock;
	pdesc[pos].events = POLLIN;
	lsockpdescpos = pos;
	pos++;
//	FD_SET(lsock,rset);
#ifdef BGJOBS
	pdesc[pos].fd = jobfd;
	pdesc[pos].events = POLLIN;
	jobfdpdescpos = pos;
	pos++;
//	FD_SET(jobfd,rset);
//	if (jobfd>max) {
//		max=jobfd;
//	}
#endif
	for (eptr=csservhead ; eptr ; eptr=eptr->next) {
		eptr->pdescpos = -1;
		eptr->fwdpdescpos = -1;
		switch (eptr->state) {
			case IDLE:
			case READ:
			case WRITELAST:
				pdesc[pos].fd = eptr->sock;
				pdesc[pos].events = 0;
				eptr->pdescpos = pos;
//				i=eptr->sock;
				if (eptr->inputpacket.bytesleft>0) {
					pdesc[pos].events |= POLLIN;
//					FD_SET(i,rset);
//					if (i>max) {
//						max=i;
//					}
				}
				if (eptr->outputhead!=NULL) {
					pdesc[pos].events |= POLLOUT;
//					FD_SET(i,wset);
//					if (i>max) {
//						max=i;
//					}
				}
				pos++;
				break;
			case CONNECTING:
				pdesc[pos].fd = eptr->fwdsock;
				pdesc[pos].events = POLLOUT;
				eptr->fwdpdescpos = pos;
				pos++;
//				i=eptr->fwdsock;
//				FD_SET(i,wset);
//				if (i>max) {
//					max=i;
//				}
				break;
			case WRITEINIT:
				if (eptr->fwdbytesleft>0) {
					pdesc[pos].fd = eptr->fwdsock;
					pdesc[pos].events = POLLOUT;
					eptr->fwdpdescpos = pos;
					pos++;
//					i=eptr->fwdsock;
//					FD_SET(i,wset);
//					if (i>max) {
//						max=i;
//					}
				}
				break;
			case WRITEFWD:
				pdesc[pos].fd = eptr->fwdsock;
				pdesc[pos].events = POLLIN;
				eptr->fwdpdescpos = pos;
//				i=eptr->fwdsock;
//				FD_SET(i,rset); // fwdsock
//				if (i>max) {
//					max=i;
//				}
				if (eptr->fwdbytesleft>0) {
					pdesc[pos].events |= POLLOUT;
//					FD_SET(i,wset);	// fwdsock
				}
				pos++;

				pdesc[pos].fd = eptr->sock;
				pdesc[pos].events = 0;
				eptr->pdescpos = pos;
//				i=eptr->sock;
				if (eptr->inputpacket.bytesleft>0) {
					pdesc[pos].events |= POLLIN;
//					FD_SET(i,rset); // sock
//					if (i>max) {
//						max=i;
//					}
				}
				if (eptr->outputhead!=NULL) {
					pdesc[pos].events |= POLLOUT;
//					FD_SET(i,wset); // sock
//					if (i>max) {
//						max=i;
//					}
				}
				pos++;
				break;
			case WRITEFINISH:
				if (eptr->outputhead!=NULL) {
					pdesc[pos].fd = eptr->sock;
					pdesc[pos].events = POLLOUT;
					eptr->pdescpos = pos;
					pos++;
//					i=eptr->sock;
//					FD_SET(i,wset);
//					if (i>max) {
//						max=i;
//					}
				}
				break;
		}
	}
	*ndesc = pos;
//	return max;
}

void csserv_serve(struct pollfd *pdesc) {
	uint32_t now=main_time();
	uint64_t usecnow=main_utime();
	csserventry *eptr,**kptr;
	packetstruct *pptr,*paptr;
#ifdef BGJOBS
	writestatus *wptr,*waptr;
	uint32_t jobscnt;
#endif
	int ns;
	uint8_t lstate;

	if (lsockpdescpos>=0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
//	if (FD_ISSET(lsock,rset)) {
		ns=tcpaccept(lsock);
		if (ns<0) {
			mfs_errlog_silent(LOG_NOTICE,"accept error");
		} else {
#ifdef BGJOBS
			if (job_pool_jobs_count(jpool)>=(BGJOBSCNT*9)/10) {
				syslog(LOG_WARNING,"jobs queue is full !!!");
				tcpclose(ns);
			} else {
#endif
				tcpnonblock(ns);
				tcpnodelay(ns);
				eptr = malloc(sizeof(csserventry));
				passert(eptr);
				eptr->next = csservhead;
				csservhead = eptr;
				eptr->state = IDLE;
				eptr->mode = HEADER;
				eptr->fwdmode = HEADER;
				eptr->sock = ns;
				eptr->fwdsock = -1;
				eptr->pdescpos = -1;
				eptr->fwdpdescpos = -1;
				eptr->activity = now;
				eptr->inputpacket.bytesleft = 8;
				eptr->inputpacket.startptr = eptr->hdrbuff;
				eptr->inputpacket.packet = NULL;
				eptr->fwdstartptr = NULL;
				eptr->fwdbytesleft = 0;
				eptr->fwdinputpacket.packet = NULL;
				eptr->fwdinitpacket = NULL;
				eptr->outputhead = NULL;
				eptr->outputtail = &(eptr->outputhead);
				eptr->chunkisopen = 0;
#ifdef BGJOBS
				eptr->wjobid = 0;
				eptr->wjobwriteid = 0;
				eptr->todolist = NULL;

				eptr->rjobid = 0;
				eptr->todocnt = 0;

				eptr->rpacket = NULL;
				eptr->wpacket = NULL;
			}
#endif
		}
	}
#ifdef BGJOBS
	if (jobfdpdescpos>=0 && (pdesc[jobfdpdescpos].revents & POLLIN)) {
//	if (FD_ISSET(jobfd,rset)) {
		job_pool_check_jobs(jpool);
	}
#endif
	for (eptr=csservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0 && (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP))) {
			eptr->state = CLOSE;
		} else if (eptr->fwdpdescpos>=0 && (pdesc[eptr->fwdpdescpos].revents & (POLLERR|POLLHUP))) {
			csserv_fwderror(eptr);
		}
		lstate = eptr->state;
		if (lstate==IDLE || lstate==READ || lstate==WRITELAST || lstate==WRITEFINISH) {
			if (eptr->pdescpos>=0 && (pdesc[eptr->pdescpos].revents & POLLIN)) {
//			if (FD_ISSET(eptr->sock,rset)) {
				eptr->activity = now;
				csserv_read(eptr);
			}
			if (eptr->pdescpos>=0 && (pdesc[eptr->pdescpos].revents & POLLOUT) && eptr->state==lstate) {
//			if (FD_ISSET(eptr->sock,wset) && eptr->state==lstate) {
				eptr->activity = now;
				csserv_write(eptr);
			}
		} else if (lstate==CONNECTING && eptr->fwdpdescpos>=0 && (pdesc[eptr->fwdpdescpos].revents & POLLOUT)) { // FD_ISSET(eptr->fwdsock,wset)) {
			eptr->activity = now;
			csserv_fwdconnected(eptr);
			if (eptr->state==WRITEINIT) {
				csserv_fwdwrite(eptr); // after connect likely some data can be send
			}
			if (eptr->state==WRITEFWD) {
				csserv_forward(eptr); // and also some data can be forwarded
			}
		} else if (eptr->state==WRITEINIT && eptr->fwdpdescpos>=0 && (pdesc[eptr->fwdpdescpos].revents & POLLOUT)) { // FD_ISSET(eptr->fwdsock,wset)) {
			eptr->activity = now;
			csserv_fwdwrite(eptr); // after sending init packet
			if (eptr->state==WRITEFWD) {
				csserv_forward(eptr); // likely some data can be forwarded
			}
		} else if (eptr->state==WRITEFWD) {
			if ((eptr->pdescpos>=0 && (pdesc[eptr->pdescpos].revents & POLLIN)) || (eptr->fwdpdescpos>=0 && (pdesc[eptr->fwdpdescpos].revents & POLLOUT))) {
//			if (FD_ISSET(eptr->fwdsock,wset) || FD_ISSET(eptr->sock,rset)) {
				eptr->activity = now;
				csserv_forward(eptr);
			}
			if (eptr->fwdpdescpos>=0 && (pdesc[eptr->fwdpdescpos].revents & POLLIN) && eptr->state==lstate) {
//			if (FD_ISSET(eptr->fwdsock,rset) && eptr->state==lstate) {
				eptr->activity = now;
				csserv_fwdread(eptr);
			}
			if (eptr->pdescpos>=0 && (pdesc[eptr->pdescpos].revents & POLLOUT) && eptr->state==lstate) {
//			if (FD_ISSET(eptr->sock,wset) && eptr->state==lstate) {
				eptr->activity = now;
				csserv_write(eptr);
			}
		}
		if (eptr->state==WRITEFINISH && eptr->outputhead==NULL) {
			eptr->state = CLOSE;
		}
		if (eptr->state==CONNECTING && eptr->connstart+CONNECT_TIMEOUT(eptr->connretrycnt)<usecnow) {
			csserv_retryconnect(eptr);
		}
		if (eptr->state!=CLOSE && eptr->state!=CLOSEWAIT && eptr->state!=CLOSED && eptr->activity+CSSERV_TIMEOUT<now) {
//			syslog(LOG_NOTICE,"timed out on state: %u",eptr->state);
			eptr->state = CLOSE;
		}
		if (eptr->state == CLOSE) {
			csserv_close(eptr);
		}
	}
#ifdef BGJOBS
	jobscnt = job_pool_jobs_count(jpool);
	if (jobscnt>=stats_maxjobscnt) {
		stats_maxjobscnt=jobscnt;
	}
#endif
	kptr = &csservhead;
	while ((eptr=*kptr)) {
		if (eptr->state == CLOSED) {
			tcpclose(eptr->sock);
			if (eptr->rpacket) {
				csserv_delete_packet(eptr->rpacket);
			}
			if (eptr->wpacket) {
				csserv_delete_preserved(eptr->wpacket);
			}
			if (eptr->fwdsock>=0) {
				tcpclose(eptr->fwdsock);
			}
			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			if (eptr->fwdinputpacket.packet) {
				free(eptr->fwdinputpacket.packet);
			}
			if (eptr->fwdinitpacket) {
				free(eptr->fwdinitpacket);
			}
#ifdef BGJOBS
			wptr = eptr->todolist;
			while (wptr) {
				waptr = wptr;
				wptr = wptr->next;
				free(waptr);
			}
#endif
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

void csserv_reload(void) {
	char *oldListenHost,*oldListenPort;
	int newlsock;

	oldListenHost = ListenHost;
	oldListenPort = ListenPort;
	ListenHost = cfg_getstr("CSSERV_LISTEN_HOST","*");
	ListenPort = cfg_getstr("CSSERV_LISTEN_PORT","9422");
	if (strcmp(oldListenHost,ListenHost)==0 && strcmp(oldListenPort,ListenPort)==0) {
		free(oldListenHost);
		free(oldListenPort);
		mfs_arg_syslog(LOG_NOTICE,"main server module: socket address hasn't changed (%s:%s)",ListenHost,ListenPort);
		return;
	}

	newlsock = tcpsocket();
	if (newlsock<0) {
		mfs_errlog(LOG_WARNING,"main server module: socket address has changed, but can't create new socket");
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
		mfs_errlog_silent(LOG_NOTICE,"main server module: can't set accept filter");
	}
	if (tcpstrlisten(newlsock,ListenHost,ListenPort,100)<0) {
		mfs_arg_errlog(LOG_ERR,"main server module: socket address has changed, but can't listen on socket (%s:%s)",ListenHost,ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		tcpclose(newlsock);
		return;
	}
	mfs_arg_syslog(LOG_NOTICE,"main server module: socket address has changed, now listen on %s:%s",ListenHost,ListenPort);
	free(oldListenHost);
	free(oldListenPort);
	tcpclose(lsock);
	lsock = newlsock;
}

int csserv_init(void) {
	ListenHost = cfg_getstr("CSSERV_LISTEN_HOST","*");
	ListenPort = cfg_getstr("CSSERV_LISTEN_PORT","9422");

	lsock = tcpsocket();
	if (lsock<0) {
		mfs_errlog(LOG_ERR,"main server module: can't create socket");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE,"main server module: can't set accept filter");
	}
	tcpresolve(ListenHost,ListenPort,&mylistenip,&mylistenport,1);
	if (tcpnumlisten(lsock,mylistenip,mylistenport,100)<0) {
		mfs_errlog(LOG_ERR,"main server module: can't listen on socket");
		return -1;
	}
	mfs_arg_syslog(LOG_NOTICE,"main server module: listen on %s:%s",ListenHost,ListenPort);

	csservhead = NULL;
	main_reloadregister(csserv_reload);
	main_destructregister(csserv_term);
	main_pollregister(csserv_desc,csserv_serve);

#ifdef BGJOBS
	jpool = job_pool_new(10,BGJOBSCNT,&jobfd);
#endif

	return 0;
}
