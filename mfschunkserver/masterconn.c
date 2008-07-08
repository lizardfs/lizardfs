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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <inttypes.h>
#include <netinet/in.h>

#include "MFSCommunication.h"
#include "datapack.h"
#include "masterconn.h"
#include "cfg.h"
#include "main.h"
#include "sockets.h"
#include "replicator.h"
#include "hddspacemgr.h"
#include "csserv.h"

#define MaxPacketSize 10000

// mode
enum {FREE,CONNECTING,HEADER,DATA,KILL};

typedef struct packetstruct {
	struct packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t *packet;
} packetstruct;

typedef struct masterconn {
	int mode;
	int sock;
	time_t lastread,lastwrite;
	uint8_t hdrbuff[8];
	packetstruct inputpacket;
	packetstruct *outputhead,**outputtail;
	uint32_t masteraddr;
} masterconn;

static masterconn *masterconnsingleton=NULL;

// from config
static uint32_t BackLogsNumber;
static char *MasterHost;
static char *MasterPort;
static uint32_t Timeout;

static uint32_t stats_bytesout=0;
static uint32_t stats_bytesin=0;

static FILE *logfd;

void masterconn_stats(uint32_t *bin,uint32_t *bout) {
	*bin = stats_bytesin;
	*bout = stats_bytesout;
	stats_bytesin = 0;
	stats_bytesout = 0;
}

uint8_t* masterconn_createpacket(masterconn *eptr,uint32_t type,uint32_t size) {
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

void masterconn_sendregister(masterconn *eptr) {
	uint8_t *buff;
	uint32_t chunks,myip;
	uint16_t myport;
	uint64_t usedspace,totalspace;
	uint64_t tdusedspace,tdtotalspace;
	uint32_t chunkcount,tdchunkcount;

	myip = csserv_getlistenip();
	myport =  csserv_getlistenport();
	hdd_get_space(&usedspace,&totalspace,&chunkcount,&tdusedspace,&tdtotalspace,&tdchunkcount);
//	syslog(LOG_NOTICE,"%llu,%llu",(unsigned long long int)usedspace,(unsigned long long int)totalspace);
	chunks = get_chunkscount();
	if (Timeout==60) {
		buff = masterconn_createpacket(eptr,CSTOMA_REGISTER,1+4+2+8+8+4+8+8+4+chunks*(8+4));
	} else {
		buff = masterconn_createpacket(eptr,CSTOMA_REGISTER,1+4+2+2+8+8+4+8+8+4+chunks*(8+4));
	}
	if (buff==NULL) {
		eptr->mode=KILL;
		return;
	}
	if (Timeout==60) {
		PUT8BIT(2,buff);	// reg version
	} else {
		PUT8BIT(3,buff);	// reg version
	}
	PUT32BIT(myip,buff);
	PUT16BIT(myport,buff);
	if (Timeout!=60) {
		PUT16BIT(Timeout,buff);
	}
	PUT64BIT(usedspace,buff);
	PUT64BIT(totalspace,buff);
	PUT32BIT(chunkcount,buff);
	PUT64BIT(tdusedspace,buff);
	PUT64BIT(tdtotalspace,buff);
	PUT32BIT(tdchunkcount,buff);
	if (chunks>0) {
		fill_chunksinfo(buff);
	}
}

void masterconn_send_space(uint64_t usedspace,uint64_t totalspace,uint32_t chunkcount,uint64_t tdusedspace,uint64_t tdtotalspace,uint32_t tdchunkcount) {
	uint8_t *buff;
	masterconn *eptr = masterconnsingleton;

//	syslog(LOG_NOTICE,"%llu,%llu",(unsigned long long int)usedspace,(unsigned long long int)totalspace);
	if (eptr->mode==DATA || eptr->mode==HEADER) {
		buff = masterconn_createpacket(eptr,CSTOMA_SPACE,8+8+4+8+8+4);
		if (buff) {
			PUT64BIT(usedspace,buff);
			PUT64BIT(totalspace,buff);
			PUT32BIT(chunkcount,buff);
			PUT64BIT(tdusedspace,buff);
			PUT64BIT(tdtotalspace,buff);
			PUT32BIT(tdchunkcount,buff);
		}
	}
}

void masterconn_send_chunk_damaged(uint64_t chunkid) {
	uint8_t *buff;
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode==DATA || eptr->mode==HEADER) {
		buff = masterconn_createpacket(eptr,CSTOMA_CHUNK_DAMAGED,8);
		if (buff) {
			PUT64BIT(chunkid,buff);
		}
	}
}

void masterconn_send_chunk_lost(uint64_t chunkid) {
	uint8_t *buff;
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode==DATA || eptr->mode==HEADER) {
		buff = masterconn_createpacket(eptr,CSTOMA_CHUNK_LOST,8);
		if (buff) {
			PUT64BIT(chunkid,buff);
		}
	}
}

void masterconn_send_error_occurred() {
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode==DATA || eptr->mode==HEADER) {
		masterconn_createpacket(eptr,CSTOMA_ERROR_OCCURRED,0);
	}
}

void masterconn_create(masterconn *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t *ptr;
	uint8_t status;

	if (length!=8+4) {
		syslog(LOG_NOTICE,"MATOCS_CREATE - wrong size (%d/12)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET32BIT(version,data);
	status = create_newchunk(chunkid,version);
	ptr = masterconn_createpacket(eptr,CSTOMA_CREATE,8+1);
	if (ptr==NULL) {
		eptr->mode=KILL;
		return;
	}
	PUT64BIT(chunkid,ptr);
	PUT8BIT(status,ptr);
}

void masterconn_delete(masterconn *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t *ptr;
	uint8_t status;

	if (length!=8+4) {
		syslog(LOG_NOTICE,"MATOCS_DELETE - wrong size (%d/12)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET32BIT(version,data);
	status = delete_chunk(chunkid,version);
	ptr = masterconn_createpacket(eptr,CSTOMA_DELETE,8+1);
	if (ptr==NULL) {
		eptr->mode=KILL;
		return;
	}
	PUT64BIT(chunkid,ptr);
	PUT8BIT(status,ptr);
}

void masterconn_setversion(masterconn *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint32_t oldversion;
	uint8_t *ptr;
	uint8_t status;
	
	if (length!=8+4+4) {
		syslog(LOG_NOTICE,"MATOCS_SET_VERSION - wrong size (%d/16)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET32BIT(version,data);
	GET32BIT(oldversion,data);
	status = set_chunk_version(chunkid,version,oldversion);
	ptr = masterconn_createpacket(eptr,CSTOMA_SET_VERSION,8+1);
	if (ptr==NULL) {
		eptr->mode=KILL;
		return;
	}
	PUT64BIT(chunkid,ptr);
	PUT8BIT(status,ptr);
}

void masterconn_duplicate(masterconn *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint64_t oldchunkid;
	uint32_t oldversion;
	uint8_t *ptr;
	uint8_t status;

	if (length!=8+4+8+4) {
		syslog(LOG_NOTICE,"MATOCS_DUPLICATE - wrong size (%d/24)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET32BIT(version,data);
	GET64BIT(oldchunkid,data);
	GET32BIT(oldversion,data);
	status = duplicate_chunk(chunkid,version,oldchunkid,oldversion);
	ptr = masterconn_createpacket(eptr,CSTOMA_DUPLICATE,8+1);
	if (ptr==NULL) {
		eptr->mode=KILL;
		return;
	}
	PUT64BIT(chunkid,ptr);
	PUT8BIT(status,ptr);
}

void masterconn_truncate(masterconn *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint32_t leng;
	uint32_t oldversion;
	uint8_t *ptr;
	uint8_t status;

	if (length!=8+4+4+4) {
		syslog(LOG_NOTICE,"MATOCS_TRUNCATE - wrong size (%d/20)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET32BIT(leng,data);
	GET32BIT(version,data);
	GET32BIT(oldversion,data);
	status = truncate_chunk(chunkid,leng,version,oldversion);
	ptr = masterconn_createpacket(eptr,CSTOMA_TRUNCATE,8+1);
	if (ptr==NULL) {
		eptr->mode=KILL;
		return;
	}
	PUT64BIT(chunkid,ptr);
	PUT8BIT(status,ptr);
}

void masterconn_duptrunc(masterconn *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint64_t oldchunkid;
	uint32_t oldversion;
	uint32_t leng;
	uint8_t *ptr;
	uint8_t status;

	if (length!=8+4+8+4+4) {
		syslog(LOG_NOTICE,"MATOCS_DUPTRUNC - wrong size (%d/28)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET32BIT(version,data);
	GET64BIT(oldchunkid,data);
	GET32BIT(oldversion,data);
	GET32BIT(leng,data);
	status = duptrunc_chunk(chunkid,version,oldchunkid,oldversion,leng);
	ptr = masterconn_createpacket(eptr,CSTOMA_DUPTRUNC,8+1);
	if (ptr==NULL) {
		eptr->mode=KILL;
		return;
	}
	PUT64BIT(chunkid,ptr);
	PUT8BIT(status,ptr);
}

void masterconn_replicate(masterconn *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint32_t ip;
	uint16_t port;

	if (length!=8+4+4+2) {
		syslog(LOG_NOTICE,"MATOCS_REPLICATE - wrong size (%d/18)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET32BIT(version,data);
	GET32BIT(ip,data);
	GET16BIT(port,data);
	replicator_new(chunkid,version,ip,port);
}

void masterconn_replicate_status(uint64_t chunkid,uint32_t version,uint8_t status) {
	uint8_t *ptr;
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode==DATA || eptr->mode==HEADER) {
		ptr = masterconn_createpacket(eptr,CSTOMA_REPLICATE,8+4+1);
		if (ptr==NULL) {
			eptr->mode=KILL;
			return;
		}
		PUT64BIT(chunkid,ptr);
		PUT32BIT(version,ptr);
		PUT8BIT(status,ptr);
	}
}

void masterconn_structure_log(masterconn *eptr,uint8_t *data,uint32_t length) {
	if (length<5) {
		syslog(LOG_NOTICE,"MATOCS_STRUCTURE_LOG - wrong size (%d/4+data)",length);
		eptr->mode = KILL;
		return;
	}
	if (data[0]==0xFF && length<10) {
		syslog(LOG_NOTICE,"MATOCS_STRUCTURE_LOG - wrong size (%d/9+data)",length);
		eptr->mode = KILL;
		return;
	}

	if (logfd==NULL) {
		logfd = fopen("changelog_csback.0.mfs","a");
	}

	data[length-1]='\0';
	if (data[0]==0xFF) {	// new version
		uint64_t version;
		data++;
		GET64BIT(version,data);
		if (logfd) {
			fprintf(logfd,"%llu: %s\n",(unsigned long long int)version,data);
		} else {
			syslog(LOG_NOTICE,"lost MFS change %llu: %s",(unsigned long long int)version,data);
		}
	} else {	// old version
		uint32_t version;
		GET32BIT(version,data);
		if (logfd) {
			fprintf(logfd,"%u: %s\n",version,data);
		} else {
			syslog(LOG_NOTICE,"lost MFS change %u: %s",version,data);
		}
	}

}

void masterconn_structure_log_rotate(masterconn *eptr,uint8_t *data,uint32_t length) {
	char logname1[100],logname2[100];
	uint32_t i;
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"MATOCS_STRUCTURE_LOG_ROTATE - wrong size (%d/0)",length);
		eptr->mode = KILL;
		return;
	}
	if (logfd==NULL) {
		fclose(logfd);
		logfd=NULL;
	}
	if (BackLogsNumber>0) {
		for (i=BackLogsNumber ; i>0 ; i--) {
			snprintf(logname1,100,"changelog_csback.%d.mfs",i);
			snprintf(logname2,100,"changelog_csback.%d.mfs",i-1);
			rename(logname2,logname1);
		}
	} else {
		unlink("changelog_csback.0.mfs");
	}
}


void masterconn_chunk_checksum(masterconn *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t *ptr;
	uint8_t status;
	uint32_t checksum;
	
	if (length!=8+4) {
		syslog(LOG_NOTICE,"ANTOCS_CHUNK_CHECKSUM - wrong size (%d/12)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET32BIT(version,data);
	status = get_chunk_checksum(chunkid,version,&checksum);
	if (status!=STATUS_OK) {
		ptr = masterconn_createpacket(eptr,CSTOAN_CHUNK_CHECKSUM,8+4+1);
	} else {
		ptr = masterconn_createpacket(eptr,CSTOAN_CHUNK_CHECKSUM,8+4+4);
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

void masterconn_chunk_checksum_tab(masterconn *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t *ptr;
	uint8_t status;
	uint8_t crctab[4096];
	
	if (length!=8+4) {
		syslog(LOG_NOTICE,"ANTOCS_CHUNK_CHECKSUM_TAB - wrong size (%d/12)",length);
		eptr->mode = KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET32BIT(version,data);
	status = get_chunk_checksum_tab(chunkid,version,crctab);
	if (status!=STATUS_OK) {
		ptr = masterconn_createpacket(eptr,CSTOAN_CHUNK_CHECKSUM_TAB,8+4+1);
	} else {
		ptr = masterconn_createpacket(eptr,CSTOAN_CHUNK_CHECKSUM_TAB,8+4+4096);
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

void masterconn_gotpacket(masterconn *eptr,uint32_t type,uint8_t *data,uint32_t length) {
	switch (type) {
		case ANTOAN_NOP:
			break;
		case MATOCS_CREATE:
			masterconn_create(eptr,data,length);
			break;
		case MATOCS_DELETE:
			masterconn_delete(eptr,data,length);
			break;
		case MATOCS_SET_VERSION:
			masterconn_setversion(eptr,data,length);
			break;
		case MATOCS_DUPLICATE:
			masterconn_duplicate(eptr,data,length);
			break;
		case MATOCS_REPLICATE:
			masterconn_replicate(eptr,data,length);
			break;
		case MATOCS_TRUNCATE:
			masterconn_truncate(eptr,data,length);
			break;
		case MATOCS_DUPTRUNC:
			masterconn_duptrunc(eptr,data,length);
			break;
		case MATOCS_STRUCTURE_LOG:
			masterconn_structure_log(eptr,data,length);
			break;
		case MATOCS_STRUCTURE_LOG_ROTATE:
			masterconn_structure_log_rotate(eptr,data,length);
			break;
		case ANTOCS_CHUNK_CHECKSUM:
			masterconn_chunk_checksum(eptr,data,length);
			break;
		case ANTOCS_CHUNK_CHECKSUM_TAB:
			masterconn_chunk_checksum_tab(eptr,data,length);
			break;
		default:
			syslog(LOG_NOTICE,"got unknown message (type:%d)",type);
			eptr->mode = KILL;
	}
}


void masterconn_term(void) {
	packetstruct *pptr,*paptr;
//	syslog(LOG_INFO,"closing %s:%s",MasterHost,MasterPort);
	masterconn *eptr = masterconnsingleton;

	if (eptr->mode!=FREE) {
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
	}

	free(eptr);
	masterconnsingleton = NULL;
}

void masterconn_connected(masterconn *eptr) {
	eptr->mode=HEADER;
	eptr->inputpacket.next = NULL;
	eptr->inputpacket.bytesleft = 8;
	eptr->inputpacket.startptr = eptr->hdrbuff;
	eptr->inputpacket.packet = NULL;
	eptr->outputhead = NULL;
	eptr->outputtail = &(eptr->outputhead);

	masterconn_sendregister(eptr);
	eptr->lastread = eptr->lastwrite = main_time();
}

void masterconn_initconnect(masterconn *eptr) {
	int status;
	eptr->sock=tcpsocket();
	if (eptr->sock<0) {
		syslog(LOG_WARNING,"create socket, error: %m");
		return ;
	}
	if (tcpnonblock(eptr->sock)<0) {
		syslog(LOG_WARNING,"set nonblock, error: %m");
		tcpclose(eptr->sock);
		eptr->sock=-1;
		return ;
	}
	status = tcpaddrconnect(eptr->sock,eptr->masteraddr);
	if (status<0) {
		syslog(LOG_WARNING,"connect failed, error: %m");
		tcpclose(eptr->sock);
		eptr->sock=-1;
	} else if (status==0) {
		syslog(LOG_NOTICE,"connected to Master immediately");
		masterconn_connected(eptr);
	} else {
		eptr->mode = CONNECTING;
		syslog(LOG_NOTICE,"connecting ...");
	}   
}   

void masterconn_connecttest(masterconn *eptr) {
	int status;

	status = tcpgetstatus(eptr->sock);
	if (status) {
		syslog(LOG_WARNING,"connection failed, error: %m");
		tcpclose(eptr->sock);
		eptr->sock=-1;
		eptr->mode=FREE;
	} else {
		syslog(LOG_NOTICE,"connected to Master");
		masterconn_connected(eptr);
	}
}

void masterconn_read(masterconn *eptr) {
	int32_t i;
	uint32_t type,size;
	uint8_t *ptr;
	i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
	if (i==0) {
		syslog(LOG_INFO,"connection lost");
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
				syslog(LOG_WARNING,"packet too long (%u/%u)",size,MaxPacketSize);
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

		masterconn_gotpacket(eptr,type,eptr->inputpacket.packet,size);

		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		eptr->inputpacket.packet=NULL;
		return;
	}

}

void masterconn_write(masterconn *eptr) {
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


int masterconn_desc(fd_set *rset,fd_set *wset) {
	int ret=0;
	masterconn *eptr = masterconnsingleton;

	if (eptr->mode==FREE || eptr->sock<0) {
		return 0;
	}
	if (eptr->mode==HEADER || eptr->mode==DATA) {
		FD_SET(eptr->sock,rset);
		ret=eptr->sock;
	}
	if (((eptr->mode==HEADER || eptr->mode==DATA) && eptr->outputhead!=NULL) || eptr->mode==CONNECTING) {
		FD_SET(eptr->sock,wset);
		ret=eptr->sock;
	}
	return ret;
}

void masterconn_serve(fd_set *rset,fd_set *wset) {
	uint32_t now=main_time();
	packetstruct *pptr,*paptr;
	masterconn *eptr = masterconnsingleton;

	if (eptr->mode==CONNECTING) {
		if (eptr->sock>=0 && FD_ISSET(eptr->sock,wset)) {
			masterconn_connecttest(eptr);
		}
	} else {
		if ((eptr->mode==HEADER || eptr->mode==DATA) && FD_ISSET(eptr->sock,rset)) {
			eptr->lastread = now;
			masterconn_read(eptr);
		}
		if ((eptr->mode==HEADER || eptr->mode==DATA) && FD_ISSET(eptr->sock,wset)) {
			eptr->lastwrite = now;
			masterconn_write(eptr);
		}
		if ((eptr->mode==HEADER || eptr->mode==DATA) && eptr->lastread+Timeout<now) {
			eptr->mode = KILL;
		}
		if ((eptr->mode==HEADER || eptr->mode==DATA) && eptr->lastwrite+(Timeout/2)<now && eptr->outputhead==NULL) {
			masterconn_createpacket(eptr,ANTOAN_NOP,0);
		}
	}
	if (eptr->mode == KILL) {
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
		eptr->mode = FREE;
	}
}

void masterconn_reconnect() {
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode==FREE) {
		masterconn_initconnect(eptr);
	}
}

int masterconn_init(void) {
	uint32_t ReconnectionDelay;
	masterconn *eptr;

	config_getuint32("MASTER_RECONNECTION_DELAY",5,&ReconnectionDelay);
	config_getnewstr("MASTER_HOST","mfsmaster",&MasterHost);
	config_getnewstr("MASTER_PORT","9420",&MasterPort);
	config_getuint32("MASTER_TIMEOUT",60,&Timeout);
	config_getuint32("BACK_LOGS",50,&BackLogsNumber);

	if (Timeout>65536) {
		Timeout=65535;
	}
	if (Timeout<=1) {
		Timeout=2;
	}
	eptr = masterconnsingleton = malloc(sizeof(masterconn));

	eptr->masteraddr = sockaddrnew(MasterHost,MasterPort,"tcp");
	eptr->mode = FREE;

	masterconn_initconnect(eptr);
	main_timeregister(ReconnectionDelay,0,masterconn_reconnect);
	main_destructregister(masterconn_term);
	main_selectregister(masterconn_desc,masterconn_serve);

	logfd = NULL;

	return 0;
}
