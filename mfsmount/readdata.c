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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <time.h>
#include <syslog.h>
#include <inttypes.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>

#include "MFSCommunication.h"
#include "sockets.h"
#include "strerr.h"
#include "mfsstrerr.h"
#include "datapack.h"
#include "mastercomm.h"
#include "cscomm.h"
#include "csdb.h"

#define USECTICK 333333

#define REFRESHTICKS 15
#define CLOSEDELAYTICKS 3

#define MAPBITS 10
#define MAPSIZE (1<<(MAPBITS))
#define MAPMASK (MAPSIZE-1)
#define MAPINDX(inode) (inode&MAPMASK)

typedef struct _readrec {
	uint8_t *rbuff;			// this->locked
	uint32_t rbuffsize;		// this->locked
	uint32_t inode;			// this->locked
	uint64_t fleng;			// this->locked
	uint32_t indx;			// this->locked
	uint64_t chunkid;		// this->locked
	uint32_t version;		// this->locked
	uint32_t ip;			// this->locked
	uint16_t port;			// this->locked
	int fd;				// this->locked
	uint8_t refcnt;			// glock
	uint8_t noaccesscnt;		// glock
	uint8_t valid;			// glock
	uint8_t locked;			// glock
	uint16_t waiting;		// glock
	pthread_cond_t cond;		// glock
	struct _readrec *next;		// glock
	struct _readrec *mapnext;	// glock
} readrec;

static readrec *rdinodemap[MAPSIZE];
static readrec *rdhead=NULL;
static pthread_t pthid;
static pthread_mutex_t glock;

static uint32_t maxretries;
static uint8_t rterm;

#define TIMEDIFF(tv1,tv2) (((int64_t)((tv1).tv_sec-(tv2).tv_sec))*1000000LL+(int64_t)((tv1).tv_usec-(tv2).tv_usec))

void* read_data_delayed_ops(void *arg) {
	readrec *rrec,**rrecp;
	readrec **rrecmap;
	(void)arg;
	for (;;) {
		pthread_mutex_lock(&glock);
		if (rterm) {
			pthread_mutex_unlock(&glock);
			return NULL;
		}
		rrecp = &rdhead;
		while ((rrec=*rrecp)!=NULL) {
			if (rrec->refcnt<REFRESHTICKS) {
				rrec->refcnt++;
			}
			if (rrec->locked==0) {
				if (rrec->valid==0) {
					pthread_cond_destroy(&(rrec->cond));
					*rrecp = rrec->next;
					rrecmap = &(rdinodemap[MAPINDX(rrec->inode)]);
					while (*rrecmap) {
						if ((*rrecmap)==rrec) {
							*rrecmap = rrec->mapnext;
						} else {
							rrecmap = &((*rrecmap)->mapnext);
						}
					}
					free(rrec);
				} else {
					if (rrec->fd>=0) {
						if (rrec->noaccesscnt==CLOSEDELAYTICKS) {
							csdb_readdec(rrec->ip,rrec->port);
							tcpclose(rrec->fd);
							rrec->fd=-1;
						} else {
							rrec->noaccesscnt++;
						}
					}
					rrecp = &(rrec->next);
				}
			} else {
				rrecp = &(rrec->next);
			}
		}
		pthread_mutex_unlock(&glock);
		usleep(USECTICK);
	}
}

void* read_data_new(uint32_t inode) {
	readrec *rrec;
	rrec = malloc(sizeof(readrec));
	rrec->rbuff = NULL;
	rrec->rbuffsize = 0;
	rrec->inode = inode;
	rrec->fleng = 0;
	rrec->indx = 0;
	rrec->chunkid = 0;
	rrec->version = 0;
	rrec->fd = -1;
	rrec->ip = 0;
	rrec->port = 0;
	rrec->refcnt = 0;
	rrec->noaccesscnt = 0;
	rrec->valid = 1;
	rrec->waiting = 0;
	rrec->locked = 0;
	pthread_cond_init(&(rrec->cond),NULL);
	pthread_mutex_lock(&glock);
	rrec->next = rdhead;
	rdhead = rrec;
	rrec->mapnext = rdinodemap[MAPINDX(inode)];
	rdinodemap[MAPINDX(inode)] = rrec;
	pthread_mutex_unlock(&glock);
//	fprintf(stderr,"read_data_new (%p)\n",rrec);
	return rrec;
}

void read_data_end(void* rr) {
	readrec *rrec = (readrec*)rr;
//	fprintf(stderr,"read_data_end (%p)\n",rr);

	pthread_mutex_lock(&glock);
	rrec->waiting++;
	while (rrec->locked) {
		pthread_cond_wait(&(rrec->cond),&glock);
	}
	rrec->waiting--;
	rrec->locked = 1;
	rrec->valid = 0;
	pthread_mutex_unlock(&glock);

	if (rrec->fd>=0) {
		csdb_readdec(rrec->ip,rrec->port);
		tcpclose(rrec->fd);
		rrec->fd=-1;
	}
	if (rrec->rbuff!=NULL) {
		free(rrec->rbuff);
		rrec->rbuff=NULL;
	}

	pthread_mutex_lock(&glock);
	if (rrec->waiting) {
		pthread_cond_signal(&(rrec->cond));
	}
	rrec->locked = 0;
	pthread_mutex_unlock(&glock);
}

void read_data_init(uint32_t retries) {
	uint32_t i;
	pthread_attr_t thattr;

	rterm = 0;
	for (i=0 ; i<MAPSIZE ; i++) {
		rdinodemap[i]=NULL;
	}
	maxretries=retries;
	pthread_mutex_init(&glock,NULL);
	pthread_attr_init(&thattr);
	pthread_attr_setstacksize(&thattr,0x100000);
	pthread_create(&pthid,&thattr,read_data_delayed_ops,NULL);
	pthread_attr_destroy(&thattr);
}

void read_data_term(void) {
	uint32_t i;
	readrec *rr,*rrn;

	pthread_mutex_lock(&glock);
	rterm = 1;
	pthread_mutex_unlock(&glock);
	pthread_join(pthid,NULL);
	pthread_mutex_destroy(&glock);
	for (i=0 ; i<MAPSIZE ; i++) {
		for (rr = rdinodemap[i] ; rr ; rr = rrn) {
			rrn = rr->next;
			if (rr->fd>=0) {
				tcpclose(rr->fd);
			}
			if (rr->rbuff!=NULL) {
				free(rr->rbuff);
			}
			pthread_cond_destroy(&(rr->cond));
			free(rr);
		}
		rdinodemap[i] = NULL;
	}
}

static int read_data_refresh_connection(readrec *rrec) {
	uint32_t ip,tmpip;
	uint16_t port,tmpport;
	uint32_t cnt,bestcnt;
	const uint8_t *csdata;
	uint32_t csdatasize;
	uint8_t status;
	uint32_t srcip;

//	fprintf(stderr,"read_data_refresh_connection (%p)\n",rrec);
	if (rrec->fd>=0) {
		csdb_readdec(rrec->ip,rrec->port);
		tcpclose(rrec->fd);
		rrec->fd = -1;
	}
	status = fs_readchunk(rrec->inode,rrec->indx,&(rrec->fleng),&(rrec->chunkid),&(rrec->version),&csdata,&csdatasize);
	if (status!=0) {
		syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - fs_readchunk returns status: %s",rrec->inode,rrec->indx,rrec->chunkid,rrec->version,mfsstrerr(status));
		if (status==ERROR_ENOENT) {
			return EBADF;	// stale handle
		}
		return EIO;
	}
//	fprintf(stderr,"(%"PRIu32",%"PRIu32",%"PRIu64",%"PRIu64",%"PRIu32",%"PRIu32",%"PRIu16")\n",rrec->inode,rrec->indx,rrec->fleng,rrec->chunkid,rrec->version,ip,port);
	if (rrec->chunkid==0 && csdata==NULL && csdatasize==0) {
		return 0;
	}
	if (csdata==NULL || csdatasize==0) {
		syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - there are no valid copies",rrec->inode,rrec->indx,rrec->chunkid,rrec->version);
		return ENXIO;
	}
	ip = 0;
	port = 0;
	// choose cs
	bestcnt = 0xFFFFFFFF;
	while (csdatasize>=6 && bestcnt>0) {
		tmpip = get32bit(&csdata);
		tmpport = get16bit(&csdata);
		csdatasize-=6;
		cnt = csdb_getopcnt(tmpip,tmpport);
		if (cnt<bestcnt) {
			ip = tmpip;
			port = tmpport;
			bestcnt = cnt;
		}
	}
	if (ip==0 || port==0) {	// this always should be false
		syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - there are no valid copies",rrec->inode,rrec->indx,rrec->chunkid,rrec->version);
		return ENXIO;
	}
	rrec->ip = ip;
	rrec->port = port;

	srcip = fs_getsrcip();
	cnt=0;
	while (cnt<10) {
		rrec->fd = tcpsocket();
		if (rrec->fd<0) {
			syslog(LOG_WARNING,"can't create tcp socket: %s",strerr(errno));
			break;
		}
		if (srcip) {
			if (tcpnumbind(rrec->fd,srcip,0)<0) {
				syslog(LOG_WARNING,"can't bind to given ip: %s",strerr(errno));
				tcpclose(rrec->fd);
				rrec->fd=-1;
				break;
			}
		}
		if (tcpnumtoconnect(rrec->fd,ip,port,(cnt%2)?(300*(1<<(cnt>>1))):(200*(1<<(cnt>>1))))<0) {
			cnt++;
			if (cnt>=10) {
				syslog(LOG_WARNING,"can't connect to (%08"PRIX32":%"PRIu16"): %s",ip,port,strerr(errno));
			}
			tcpclose(rrec->fd);
			rrec->fd=-1;
		} else {
			cnt=10;
		}
	}
	if (rrec->fd<0) {
		return EIO;
	}

	if (tcpnodelay(rrec->fd)<0) {
		syslog(LOG_WARNING,"can't set TCP_NODELAY: %s",strerr(errno));
	}

	csdb_readinc(rrec->ip,rrec->port);
	pthread_mutex_lock(&glock);
	rrec->refcnt = 0;
	pthread_mutex_unlock(&glock);
	return 0;
}

void read_inode_ops(uint32_t inode) {	// attributes of inode have been changed - force reconnect
	readrec *rrec;
	pthread_mutex_lock(&glock);
	for (rrec = rdinodemap[MAPINDX(inode)] ; rrec ; rrec=rrec->mapnext) {
		if (rrec->inode==inode) {
			rrec->noaccesscnt=CLOSEDELAYTICKS;	// if no access then close socket as soon as possible
			rrec->refcnt=REFRESHTICKS;		// force reconnect on forthcoming access
		}
	}
	pthread_mutex_unlock(&glock);
}

int read_data(void *rr, uint64_t offset, uint32_t *size, uint8_t **buff) {
	uint8_t *buffptr;
	uint64_t curroff;
	uint32_t currsize;
	uint32_t indx;
	uint8_t cnt,eb,forcereconnect;
	uint32_t chunkoffset;
	uint32_t chunksize;
	int err;
	readrec *rrec = (readrec*)rr;

	if (*size==0 && *buff!=NULL) {
		return 0;
	}

	pthread_mutex_lock(&glock);
	rrec->waiting++;
	while (rrec->locked) {
		pthread_cond_wait(&(rrec->cond),&glock);
	}
	rrec->waiting--;
	rrec->locked=1;
	forcereconnect = (rrec->fd>=0 && rrec->refcnt==REFRESHTICKS)?1:0;
	pthread_mutex_unlock(&glock);

	if (forcereconnect) {
		csdb_readdec(rrec->ip,rrec->port);
		tcpclose(rrec->fd);
		rrec->fd=-1;
	}

	if (*size==0) {
		return 0;
	}

	eb=1;
	if (*buff==NULL) {	// use internal buffer
		eb=0;
		if (*size>rrec->rbuffsize) {
			if (rrec->rbuff!=NULL) {
				free(rrec->rbuff);
			}
			rrec->rbuffsize = *size;
			rrec->rbuff = malloc(rrec->rbuffsize);
			if (rrec->rbuff==NULL) {
				rrec->rbuffsize = 0;
				syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - out of memory",rrec->inode,rrec->indx);
				return ENOMEM;	// out of memory
			}
		}
	}

	err = EIO;
	cnt = 0;
	if (*buff==NULL) {
		buffptr = rrec->rbuff;
	} else {
		buffptr = *buff;
	}
	curroff = offset;
	currsize = *size;
	while (currsize>0) {
		indx = (curroff>>MFSCHUNKBITS);
		if (rrec->fd<0 || rrec->indx != indx) {
			rrec->indx = indx;
			while (cnt<maxretries) {
				cnt++;
				err = read_data_refresh_connection(rrec);
				if (err==0) {
					break;
				}
				syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - can't connect to proper chunkserver (try counter: %"PRIu32")",rrec->inode,rrec->indx,cnt);
				if (err==EBADF) {	// no such inode - it's unrecoverable error
					if (eb) {
						pthread_mutex_lock(&glock);
						if (rrec->waiting) {
							pthread_cond_signal(&(rrec->cond));
						}
						rrec->locked = 0;
						pthread_mutex_unlock(&glock);
//						pthread_mutex_unlock(&(rrec->lock));
					}
					return err;
				}
				if (err==ENXIO) {	// chunk not available - unrecoverable, but wait longer, and make less retries
					sleep(60);
					cnt+=6;
				} else {
					sleep(1+((cnt<30)?(cnt/3):10));
				}
			}
			if (cnt>=maxretries) {
				if (eb) {
					pthread_mutex_lock(&glock);
					if (rrec->waiting) {
						pthread_cond_signal(&(rrec->cond));
					}
					rrec->locked=0;
					pthread_mutex_unlock(&glock);
//					pthread_mutex_unlock(&(rrec->lock));
				}
				return err;
			}
		}
		if (curroff>=rrec->fleng) {
			break;
		}
		if (curroff+currsize>rrec->fleng) {
			currsize = rrec->fleng-curroff;
		}
		chunkoffset = (curroff&MFSCHUNKMASK);
		if (chunkoffset+currsize>MFSCHUNKSIZE) {
			chunksize = MFSCHUNKSIZE-chunkoffset;
		} else {
			chunksize = currsize;
		}
		if (rrec->chunkid>0) {
			// fprintf(stderr,"(%d,%"PRIu64",%"PRIu32",%"PRIu32",%"PRIu32",%p)\n",rrec->fd,rrec->chunkid,rrec->version,chunkoffset,chunksize,buffptr);
			if (cs_readblock(rrec->fd,rrec->chunkid,rrec->version,chunkoffset,chunksize,buffptr)<0) {
				syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32", cs: %08"PRIX32":%"PRIu16" - readblock error (try counter: %"PRIu32")",rrec->inode,rrec->indx,rrec->chunkid,rrec->version,rrec->ip,rrec->port,cnt);
				csdb_readdec(rrec->ip,rrec->port);
				tcpclose(rrec->fd);
				rrec->fd = -1;
				sleep(1+((cnt<30)?(cnt/3):10));
			} else {
				curroff+=chunksize;
				currsize-=chunksize;
				buffptr+=chunksize;
			}
		} else {
			memset(buffptr,0,chunksize);
			curroff+=chunksize;
			currsize-=chunksize;
			buffptr+=chunksize;
		}
	}

	if (rrec->fleng<=offset) {
		*size = 0;
	} else if (rrec->fleng<(offset+(*size))) {
		if (*buff==NULL) {
			*buff = rrec->rbuff;
		}
		*size = rrec->fleng - offset;
	} else {
		if (*buff==NULL) {
			*buff = rrec->rbuff;
		}
	}
	pthread_mutex_lock(&glock);
	rrec->noaccesscnt=0;
	if (eb) {
		if (rrec->waiting) {
			pthread_cond_signal(&(rrec->cond));
		}
		rrec->locked = 0;
//		pthread_mutex_unlock(&(rrec->lock));
	}
	pthread_mutex_unlock(&glock);
	return 0;
}

void read_data_freebuff(void *rr) {
	readrec *rrec = (readrec*)rr;
	pthread_mutex_lock(&glock);
	if (rrec->waiting) {
		pthread_cond_signal(&(rrec->cond));
	}
	rrec->locked = 0;
	pthread_mutex_unlock(&glock);
//	pthread_mutex_unlock(&(rrec->lock));
}
