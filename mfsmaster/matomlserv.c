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

#include <time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>

#include "MFSCommunication.h"

#include "datapack.h"
#include "matomlserv.h"
#include "crc.h"
#include "cfg.h"
#include "main.h"
#include "sockets.h"
#include "slogger.h"
#include "massert.h"

#define MaxPacketSize 1500000

// matomlserventry.mode
enum{KILL,HEADER,DATA};

typedef struct packetstruct {
	struct packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t *packet;
} packetstruct;

typedef struct matomlserventry {
	uint8_t mode;
	int sock;
	int32_t pdescpos;
	uint32_t lastread,lastwrite;
	uint8_t hdrbuff[8];
	packetstruct inputpacket;
	packetstruct *outputhead,**outputtail;

	uint16_t timeout;

	char *servstrip;		// human readable version of servip
	uint32_t version;
	uint32_t servip;

	int metafd,chain1fd,chain2fd;

	struct matomlserventry *next;
} matomlserventry;

static matomlserventry *matomlservhead=NULL;
static int lsock;
static int32_t lsockpdescpos;

// from config
static char *ListenHost;
static char *ListenPort;


uint32_t matomlserv_mloglist_size(void) {
	matomlserventry *eptr;
	uint32_t i;
	i=0;
	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL) {
			i++;
		}
	}
	return i*(4+4);
}

void matomlserv_mloglist_data(uint8_t *ptr) {
	matomlserventry *eptr;
	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL) {
			put32bit(&ptr,eptr->version);
			put32bit(&ptr,eptr->servip);
		}
	}
}

void matomlserv_status(void) {
	matomlserventry *eptr;
	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode==HEADER || eptr->mode==DATA) {
			return;
		}
	}
	syslog(LOG_WARNING,"no meta loggers connected !!!");
}

char* matomlserv_makestrip(uint32_t ip) {
	uint8_t *ptr,pt[4];
	uint32_t l,i;
	char *optr;
	ptr = pt;
	put32bit(&ptr,ip);
	l=0;
	for (i=0 ; i<4 ; i++) {
		if (pt[i]>=100) {
			l+=3;
		} else if (pt[i]>=10) {
			l+=2;
		} else {
			l+=1;
		}
	}
	l+=4;
	optr = malloc(l);
	passert(optr);
	snprintf(optr,l,"%"PRIu8".%"PRIu8".%"PRIu8".%"PRIu8,pt[0],pt[1],pt[2],pt[3]);
	optr[l-1]=0;
	return optr;
}

uint8_t* matomlserv_createpacket(matomlserventry *eptr,uint32_t type,uint32_t size) {
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

void matomlserv_register(matomlserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t rversion;

	if (eptr->version>0) {
		syslog(LOG_WARNING,"got register message from registered metalogger !!!");
		eptr->mode=KILL;
		return;
	}
	if (length<1) {
		syslog(LOG_NOTICE,"MLTOMA_REGISTER - wrong size (%"PRIu32")",length);
		eptr->mode=KILL;
		return;
	} else {
		rversion = get8bit(&data);
		if (rversion==1) {
			if (length!=7) {
				syslog(LOG_NOTICE,"MLTOMA_REGISTER (ver 1) - wrong size (%"PRIu32"/7)",length);
				eptr->mode=KILL;
				return;
			}
			eptr->version = get32bit(&data);
			eptr->timeout = get16bit(&data);
			if (eptr->timeout<10) {
				syslog(LOG_NOTICE,"MLTOMA_REGISTER communication timeout too small (%"PRIu16" seconds - should be at least 10 seconds)",eptr->timeout);
				if (eptr->timeout<3) {
					eptr->timeout=3;
				}
//				eptr->mode=KILL;
				return;
			}
		} else {
			syslog(LOG_NOTICE,"MLTOMA_REGISTER - wrong version (%"PRIu8"/1)",rversion);
			eptr->mode=KILL;
			return;
		}
	}
}

void matomlserv_download_start(matomlserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t filenum;
	uint64_t size;
	uint8_t *ptr;
	if (length!=1) {
		syslog(LOG_NOTICE,"MLTOMA_DOWNLOAD_START - wrong size (%"PRIu32"/1)",length);
		eptr->mode=KILL;
		return;
	}
	filenum = get8bit(&data);
	if (filenum==1 || filenum==2) {
		if (eptr->metafd>=0) {
			close(eptr->metafd);
			eptr->metafd=-1;
		}
		if (eptr->chain1fd>=0) {
			close(eptr->chain1fd);
			eptr->chain1fd=-1;
		}
		if (eptr->chain2fd>=0) {
			close(eptr->chain2fd);
			eptr->chain2fd=-1;
		}
	}
	if (filenum==1) {
		eptr->metafd = open("metadata.mfs.back",O_RDONLY);
		eptr->chain1fd = open("changelog.0.mfs",O_RDONLY);
		eptr->chain2fd = open("changelog.1.mfs",O_RDONLY);
	} else if (filenum==2) {
		eptr->metafd = open("sessions.mfs",O_RDONLY);
	} else if (filenum==11) {
		if (eptr->metafd>=0) {
			close(eptr->metafd);
		}
		eptr->metafd = eptr->chain1fd;
		eptr->chain1fd = -1;
	} else if (filenum==12) {
		if (eptr->metafd>=0) {
			close(eptr->metafd);
		}
		eptr->metafd = eptr->chain2fd;
		eptr->chain2fd = -1;
	} else {
		eptr->mode=KILL;
		return;
	}
	if (eptr->metafd<0) {
		if (filenum==11 || filenum==12) {
			ptr = matomlserv_createpacket(eptr,MATOML_DOWNLOAD_START,8);
			put64bit(&ptr,0);
			return;
		} else {
			ptr = matomlserv_createpacket(eptr,MATOML_DOWNLOAD_START,1);
			put8bit(&ptr,0xff);	// error
			return;
		}
	}
	size = lseek(eptr->metafd,0,SEEK_END);
	ptr = matomlserv_createpacket(eptr,MATOML_DOWNLOAD_START,8);
	put64bit(&ptr,size);	// ok
}

void matomlserv_download_data(matomlserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint64_t offset;
	uint32_t leng;
	uint32_t crc;
	ssize_t ret;

	if (length!=12) {
		syslog(LOG_NOTICE,"MLTOMA_DOWNLOAD_DATA - wrong size (%"PRIu32"/12)",length);
		eptr->mode=KILL;
		return;
	}
	if (eptr->metafd<0) {
		syslog(LOG_NOTICE,"MLTOMA_DOWNLOAD_DATA - file not opened");
		eptr->mode=KILL;
		return;
	}
	offset = get64bit(&data);
	leng = get32bit(&data);
	ptr = matomlserv_createpacket(eptr,MATOML_DOWNLOAD_DATA,16+leng);
	put64bit(&ptr,offset);
	put32bit(&ptr,leng);
#ifdef HAVE_PREAD
	ret = pread(eptr->metafd,ptr+4,leng,offset);
#else /* HAVE_PWRITE */
	lseek(eptr->metafd,offset,SEEK_SET);
	ret = read(eptr->metafd,ptr+4,leng);
#endif /* HAVE_PWRITE */
	if (ret!=(ssize_t)leng) {
		mfs_errlog_silent(LOG_NOTICE,"error reading metafile");
		eptr->mode=KILL;
		return;
	}
	crc = mycrc32(0,ptr+4,leng);
	put32bit(&ptr,crc);
}

void matomlserv_download_end(matomlserventry *eptr,const uint8_t *data,uint32_t length) {
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"MLTOMA_DOWNLOAD_END - wrong size (%"PRIu32"/0)",length);
		eptr->mode=KILL;
		return;
	}
	if (eptr->metafd>=0) {
		close(eptr->metafd);
		eptr->metafd=-1;
	}
}

void matomlserv_broadcast_logstring(uint64_t version,uint8_t *logstr,uint32_t logstrsize) {
	matomlserventry *eptr;
	uint8_t *data;

	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->version>0) {
			data = matomlserv_createpacket(eptr,MATOML_METACHANGES_LOG,9+logstrsize);
			put8bit(&data,0xFF);
			put64bit(&data,version);
			memcpy(data,logstr,logstrsize);
		}
	}
}

void matomlserv_broadcast_logrotate() {
	matomlserventry *eptr;
	uint8_t *data;

	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->version>0) {
			data = matomlserv_createpacket(eptr,MATOML_METACHANGES_LOG,1);
			put8bit(&data,0x55);
		}
	}
}

void matomlserv_beforeclose(matomlserventry *eptr) {
	if (eptr->metafd>=0) {
		close(eptr->metafd);
		eptr->metafd=-1;
	}
	if (eptr->chain1fd>=0) {
		close(eptr->chain1fd);
		eptr->chain1fd=-1;
	}
	if (eptr->chain2fd>=0) {
		close(eptr->chain2fd);
		eptr->chain2fd=-1;
	}
}

void matomlserv_gotpacket(matomlserventry *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
	switch (type) {
		case ANTOAN_NOP:
			break;
		case MLTOMA_REGISTER:
			matomlserv_register(eptr,data,length);
			break;
		case MLTOMA_DOWNLOAD_START:
			matomlserv_download_start(eptr,data,length);
			break;
		case MLTOMA_DOWNLOAD_DATA:
			matomlserv_download_data(eptr,data,length);
			break;
		case MLTOMA_DOWNLOAD_END:
			matomlserv_download_end(eptr,data,length);
			break;
		default:
			syslog(LOG_NOTICE,"master <-> metaloggers module: got unknown message (type:%"PRIu32")",type);
			eptr->mode=KILL;
	}
}

void matomlserv_term(void) {
	matomlserventry *eptr,*eaptr;
	packetstruct *pptr,*paptr;
	syslog(LOG_INFO,"master <-> metaloggers module: closing %s:%s",ListenHost,ListenPort);
	tcpclose(lsock);

	eptr = matomlservhead;
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
	matomlservhead=NULL;

	free(ListenHost);
	free(ListenPort);
}

void matomlserv_read(matomlserventry *eptr) {
	int32_t i;
	uint32_t type,size;
	const uint8_t *ptr;
	for (;;) {
		i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
		if (i==0) {
			syslog(LOG_NOTICE,"connection with ML(%s) has been closed by peer",eptr->servstrip);
			eptr->mode = KILL;
			return;
		}
		if (i<0) {
			if (errno!=EAGAIN) {
				mfs_arg_errlog_silent(LOG_NOTICE,"read from ML(%s) error",eptr->servstrip);
				eptr->mode = KILL;
			}
			return;
		}
		eptr->inputpacket.startptr+=i;
		eptr->inputpacket.bytesleft-=i;

		if (eptr->inputpacket.bytesleft>0) {
			return;
		}

		if (eptr->mode==HEADER) {
			ptr = eptr->hdrbuff+4;
			size = get32bit(&ptr);

			if (size>0) {
				if (size>MaxPacketSize) {
					syslog(LOG_WARNING,"ML(%s) packet too long (%"PRIu32"/%u)",eptr->servstrip,size,MaxPacketSize);
					eptr->mode = KILL;
					return;
				}
				eptr->inputpacket.packet = malloc(size);
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

			matomlserv_gotpacket(eptr,type,eptr->inputpacket.packet,size);

			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet=NULL;
		}
	}
}

void matomlserv_write(matomlserventry *eptr) {
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
				mfs_arg_errlog_silent(LOG_NOTICE,"write to ML(%s) error",eptr->servstrip);
				eptr->mode = KILL;
			}
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
}

void matomlserv_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	matomlserventry *eptr;
	pdesc[pos].fd = lsock;
	pdesc[pos].events = POLLIN;
	lsockpdescpos = pos;
	pos++;
	for (eptr=matomlservhead ; eptr ; eptr=eptr->next) {
		pdesc[pos].fd = eptr->sock;
		pdesc[pos].events = POLLIN;
		eptr->pdescpos = pos;
		if (eptr->outputhead!=NULL) {
			pdesc[pos].events |= POLLOUT;
		}
		pos++;
	}
	*ndesc = pos;
}

void matomlserv_serve(struct pollfd *pdesc) {
	uint32_t now=main_time();
	matomlserventry *eptr,**kptr;
	packetstruct *pptr,*paptr;
	int ns;

	if (lsockpdescpos>=0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
		ns=tcpaccept(lsock);
		if (ns<0) {
			mfs_errlog_silent(LOG_NOTICE,"Master<->ML socket: accept error");
		} else {
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = malloc(sizeof(matomlserventry));
			passert(eptr);
			eptr->next = matomlservhead;
			matomlservhead = eptr;
			eptr->sock = ns;
			eptr->pdescpos = -1;
			eptr->mode = HEADER;
			eptr->lastread = now;
			eptr->lastwrite = now;
			eptr->inputpacket.next = NULL;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;
			eptr->inputpacket.packet = NULL;
			eptr->outputhead = NULL;
			eptr->outputtail = &(eptr->outputhead);
			eptr->timeout = 10;

			tcpgetpeer(eptr->sock,&(eptr->servip),NULL);
			eptr->servstrip = matomlserv_makestrip(eptr->servip);
			eptr->version=0;
			eptr->metafd=-1;
			eptr->chain1fd=-1;
			eptr->chain2fd=-1;
		}
	}
	for (eptr=matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP)) {
				eptr->mode = KILL;
			}
			if ((pdesc[eptr->pdescpos].revents & POLLIN) && eptr->mode!=KILL) {
				eptr->lastread = now;
				matomlserv_read(eptr);
			}
			if ((pdesc[eptr->pdescpos].revents & POLLOUT) && eptr->mode!=KILL) {
				eptr->lastwrite = now;
				matomlserv_write(eptr);
			}
		}
		if ((uint32_t)(eptr->lastread+eptr->timeout)<(uint32_t)now) {
			eptr->mode = KILL;
		}
		if ((uint32_t)(eptr->lastwrite+(eptr->timeout/3))<(uint32_t)now && eptr->outputhead==NULL) {
			matomlserv_createpacket(eptr,ANTOAN_NOP,0);
		}
	}
	kptr = &matomlservhead;
	while ((eptr=*kptr)) {
		if (eptr->mode == KILL) {
			matomlserv_beforeclose(eptr);
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
			if (eptr->servstrip) {
				free(eptr->servstrip);
			}
			*kptr = eptr->next;
			free(eptr);
		} else {
			kptr = &(eptr->next);
		}
	}
}

void matomlserv_reload(void) {
	char *oldListenHost,*oldListenPort;
	int newlsock;

	oldListenHost = ListenHost;
	oldListenPort = ListenPort;
	ListenHost = cfg_getstr("MATOML_LISTEN_HOST","*");
	ListenPort = cfg_getstr("MATOML_LISTEN_PORT","9419");
	if (strcmp(oldListenHost,ListenHost)==0 && strcmp(oldListenPort,ListenPort)==0) {
		free(oldListenHost);
		free(oldListenPort);
		mfs_arg_syslog(LOG_NOTICE,"master <-> metaloggers module: socket address hasn't changed (%s:%s)",ListenHost,ListenPort);
		return;
	}

	newlsock = tcpsocket();
	if (newlsock<0) {
		mfs_errlog(LOG_WARNING,"master <-> metaloggers module: socket address has changed, but can't create new socket");
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
		mfs_errlog_silent(LOG_NOTICE,"master <-> metaloggers module: can't set accept filter");
	}
	if (tcpstrlisten(newlsock,ListenHost,ListenPort,100)<0) {
		mfs_arg_errlog(LOG_ERR,"master <-> metaloggers module: socket address has changed, but can't listen on socket (%s:%s)",ListenHost,ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		tcpclose(newlsock);
		return;
	}
	mfs_arg_syslog(LOG_NOTICE,"master <-> metaloggers module: socket address has changed, now listen on %s:%s",ListenHost,ListenPort);
	free(oldListenHost);
	free(oldListenPort);
	tcpclose(lsock);
	lsock = newlsock;
}

int matomlserv_init(void) {
	ListenHost = cfg_getstr("MATOML_LISTEN_HOST","*");
	ListenPort = cfg_getstr("MATOML_LISTEN_PORT","9419");

	lsock = tcpsocket();
	if (lsock<0) {
		mfs_errlog(LOG_ERR,"master <-> metaloggers module: can't create socket");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE,"master <-> metaloggers module: can't set accept filter");
	}
	if (tcpstrlisten(lsock,ListenHost,ListenPort,100)<0) {
		mfs_errlog(LOG_ERR,"master <-> metaloggers module: can't listen on socket");
		return -1;
	}
	mfs_arg_syslog(LOG_NOTICE,"master <-> metaloggers module: listen on %s:%s",ListenHost,ListenPort);

	matomlservhead = NULL;
	main_reloadregister(matomlserv_reload);
	main_destructregister(matomlserv_term);
	main_pollregister(matomlserv_desc,matomlserv_serve);
	main_timeregister(TIMEMODE_SKIP_LATE,3600,0,matomlserv_status);
	return 0;
}
